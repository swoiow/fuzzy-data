from collections import namedtuple

from constant import *
from factories import *
from fuzzy_models import PATCH_COLUMNS


fuzzyParams = namedtuple("fuzzyParams", ["func", "kwargs"])

PATCH_COLUMNS["cpl"] = {
    "created": fuzzyParams(fuzzyDateTime, {}),
    "content_kind": fuzzyParams(fuzzyEnumOne, dict(bucket=CPL_CONTENT_KIND)),
    "audio_formats": fuzzyParams(fuzzyEnum, dict(bucket=CPL_AUDIO_FORMATS, size=1)),
    "frame_rate": fuzzyParams(fuzzyEnumOne, dict(bucket=CPL_FRAME_RATE)),
    "edit_rate": fuzzyParams(fuzzyEnumOne, dict(bucket=CPL_EDIT_RATE)),
    "audio_language": fuzzyParams(fuzzyEnumOne, dict(bucket=CPL_AUDIO_LANGUAGE)),
    "aspect_ratio_active_area": fuzzyParams(fuzzyEnumOne, dict(bucket=CPL_ASPECT_RATIO_ACTIVE_AREA)),
}
