import tarfile
from typing import TYPE_CHECKING, Optional, IO, Tuple, Dict, Mapping
import posixpath
import tempfile
import io
import logging

import fsspec
import torchx
from torchx.specs.api import CfgVal, AppDef

if TYPE_CHECKING:
    from docker import DockerClient

LABEL_VERSION = "torchx.pytorch.org/version"

logger: logging.Logger = logging.getLogger(__name__)

def _copy_to_tarfile(workspace: str, tf: tarfile.TarFile) -> None:
    # TODO(d4l3k) implement docker ignore files

    fs, path = fsspec.core.url_to_fs(workspace)
    assert isinstance(path, str), "path must be str"

    for dir, dirs, files in fs.walk(path, detail=True):
        assert isinstance(dir, str), "path must be str"
        relpath = posixpath.relpath(dir, path)
        for file, info in files.items():
            with fs.open(info["name"], "rb") as f:
                tinfo = tarfile.TarInfo(posixpath.join(relpath, file))
                tinfo.size = info["size"]
                tf.addfile(tinfo, f)


def _build_context(img: str, workspace: str) -> IO[bytes]:
    # f is closed by parent, NamedTemporaryFile auto closes on GC
    f = tempfile.NamedTemporaryFile(  # noqa P201
        prefix="torchx-context",
        suffix=".tar",
    )
    dockerfile = bytes(f"FROM {img}\nCOPY . .\n", encoding="utf-8")
    with tarfile.open(fileobj=f, mode="w") as tf:
        info = tarfile.TarInfo("Dockerfile")
        info.size = len(dockerfile)
        tf.addfile(info, io.BytesIO(dockerfile))

        _copy_to_tarfile(workspace, tf)

    f.seek(0)
    return f


class DockerPatcher:
    def __init__(self, client: Optional["DockerClient"] = None) -> None:
        self.__client = client

    def _client(self) -> "DockerClient":
        client = self.__client
        if client is None:
            import docker

            client = docker.from_env()
            self.__client = client
        return client


    def build_container_from_workspace(
        self, img: str, workspace: str
    ) -> str:
        """
        build_container_from_workspace creates a new Docker container with the
        workspace filesystem applied as a layer on top of the provided base image.
        """
        context = _build_context(img, workspace)

        try:
            image, _ = self._client().images.build(
                fileobj=context,
                custom_context=True,
                pull=True,
                rm=True,
                labels={
                    LABEL_VERSION: torchx.__version__,
                },
            )
        finally:
            context.close()

        return image.id

    def update_app_images(self, app: AppDef, cfg: Mapping[str, CfgVal]) -> Dict[str, Tuple[str, str]]:
        HASH_PREFIX = "sha256:"

        images_to_push = {}
        for role in app.roles:
            if role.image.startswith(HASH_PREFIX):
                image_repo = cfg.get("image_repo")
                if not image_repo:
                    raise KeyError(
                        f"must specify the image repository via `image_repo` config to be able to upload local image {role.image}"
                    )
                assert isinstance(image_repo, str), "image_repo must be str"

                image_hash = role.image[len(HASH_PREFIX) :]
                remote_image = image_repo + ":" + image_hash
                images_to_push[role.image] = (
                    image_repo,
                    image_hash,
                )
                role.image = remote_image
        return images_to_push

    def push_images(self, images_to_push: Dict[str, Tuple[str, str]]) -> None:
        if len(images_to_push) == 0:
            return

        client = self._client()
        for local, (repo, tag) in images_to_push.items():
            logger.info(f"pushing image {repo}:{tag}...")
            img = client.images.get(local)
            img.tag(repo, tag=tag)
            for line in client.images.push(repo, tag=tag, stream=True, decode=True):
                ERROR_KEY = "error"
                if ERROR_KEY in line:
                    raise RuntimeError(
                        f"failed to push docker image: {line[ERROR_KEY]}"
                    )
                logger.info(f"docker: {line}")
