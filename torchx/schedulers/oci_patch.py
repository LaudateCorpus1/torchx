from opencontainers.distribution import reggie

import requests
import io
import tarfile
import hashlib
import os
import os.path
import json
import gzip


dst_name = "torchx"
dst_ref = "tristanr_patched"

src_endpoint = "https://ghcr.io"
dst_endpoint = "https://495572122715.dkr.ecr.us-west-2.amazonaws.com"

src = reggie.NewClient(
    src_endpoint,
    reggie.WithDefaultName("pytorch/torchx"),
)

with open("ecr.passwd", "rt") as f:
    password = f.read()

dst_auth = ("AWS", password)
dst = reggie.NewClient(
    dst_endpoint,
    reggie.WithUsernamePassword("AWS", password),
)

req = src.NewRequest(
    "GET",
    "/v2/<name>/manifests/<reference>",
    reggie.WithReference("0.1.2dev0"),
)
resp = src.Do(req)
manifest = resp.json()
print(manifest)
layers = manifest["layers"]
config_digest = manifest["config"]["digest"]


def get_blob_raw(digest):
    req = src.NewRequest("GET", "/v2/<name>/blobs/<digest>", reggie.WithDigest(digest))
    req.stream = True
    return src.Do(req)


def get_blob(digest):
    return get_blob_raw(digest).json()


config = get_blob(config_digest)
print(config)

wd = config["container_config"]["WorkingDir"]

PATCH_FILE = "patch.tar.gz"
with tarfile.open(PATCH_FILE, mode="w:gz") as tf:
    content = b"blah blah"
    info = tarfile.TarInfo(os.path.join(wd, "test.txt"))
    info.size = len(content)
    tf.addfile(info, io.BytesIO(content))


def digest_str(s):
    m = hashlib.sha256()
    m.update(s)
    return "sha256:" + m.hexdigest()


def compute_digest(reader):
    m = hashlib.sha256()
    patch_size = 0
    while True:
        data = f.read(64000)
        if not data:
            break
        m.update(data)
        patch_size += len(data)
    patch_digest = "sha256:" + m.hexdigest()
    return patch_digest, patch_size


with open(PATCH_FILE, "rb") as f:
    patch_digest, patch_size = compute_digest(f)

with gzip.open(PATCH_FILE, "rb") as f:
    diff_digest, _ = compute_digest(f)


manifest["layers"].append(
    {
        "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
        "size": patch_size,
        "digest": patch_digest,
    }
)


def blob_exists(digest):
    resp = requests.head(
        dst_endpoint + f"/v2/{dst_name}/blobs/{digest}",
        auth=dst_auth,
    )
    return resp.status_code == requests.codes.ok


def upload(digest, blob):
    if hasattr(blob, "__len__"):
        size = len(blob)
    else:
        size = os.fstat(blob.fileno()).st_size
    print(f"uploading {digest}, len {size}")
    resp = requests.post(
        dst_endpoint + f"/v2/{dst_name}/blobs/uploads/?digest={digest}",
        data=blob,
        headers={
            "Content-Length": str(size),
        },
        auth=dst_auth,
    )
    resp.raise_for_status()


def upload_manifest(manifest):
    resp = requests.put(
        dst_endpoint + f"/v2/{dst_name}/manifests/{dst_ref}",
        headers={
            "Content-Type": manifest["mediaType"],
        },
        data=json.dumps(manifest),
        auth=dst_auth,
    )
    if resp.status_code != requests.codes.ok:
        print(resp.content)
    resp.raise_for_status()
    print(resp, resp.headers)


with open(PATCH_FILE, "rb") as f:
    upload(patch_digest, f)


class ResponseReader:
    def __init__(self, resp):
        self.resp = resp
        self.mode = "rb"

    def read(self, n):
        return self.resp.raw.read(n)

    def __len__(self):
        return int(self.resp.headers["Content-Length"])


to_upload = [layer["digest"] for layer in manifest["layers"]]

config["rootfs"]["diff_ids"].append(diff_digest)
config_json = json.dumps(config)
config_digest = digest_str(config_json.encode("utf-8"))
upload(config_digest, config_json)
manifest["config"]["digest"] = config_digest


for digest in to_upload:
    if blob_exists(digest):
        print(f"blob exists {digest}")
        continue
    resp = get_blob_raw(digest)
    reader = ResponseReader(resp)
    upload(digest, reader)


upload_manifest(manifest)
