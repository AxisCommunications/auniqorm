from hatchling.metadata.plugin.interface import MetadataHookInterface


class PinNativeHook(MetadataHookInterface):
    """Pin the native package to aUniqORM's minor version."""

    def update(self, metadata: dict) -> None:
        major, minor, *_ = metadata["version"].split(".")
        metadata["dependencies"] = [
            "auniqorm-native~=" + f"{major}.{minor}.0",
            "msgspec>=0.22",
        ]
