import io
import unittest
from unittest.mock import patch

from PIL import Image

from docreader.main import _iter_image_refs, _normalize_image_for_vlm, _resolve_images


def _bmp_bytes() -> bytes:
    out = io.BytesIO()
    Image.new("RGB", (80, 60), color=(20, 40, 60)).save(out, format="BMP")
    return out.getvalue()


class ImageNormalizationTest(unittest.TestCase):
    def test_unsupported_raster_format_is_converted_to_png(self):
        normalized = _normalize_image_for_vlm("figure.bmp", "image/bmp", _bmp_bytes())

        self.assertIsNotNone(normalized)
        filename, mime_type, image_data = normalized
        self.assertEqual(filename, "figure.png")
        self.assertEqual(mime_type, "image/png")
        self.assertTrue(image_data.startswith(b"\x89PNG\r\n\x1a\n"))

    @patch("docreader.main.rasterize_media_bytes", return_value=None)
    def test_unconvertible_vector_image_is_not_forwarded(self, _rasterize):
        images = {"images/figure.x-wmf": b"not-a-real-metafile"}

        _, refs = _resolve_images(images.copy(), "request-id")
        streamed = list(_iter_image_refs(images.copy()))

        self.assertEqual(refs, [])
        self.assertEqual(streamed, [])


if __name__ == "__main__":
    unittest.main()
