import unittest
from unittest.mock import patch

from docreader.parser.pptx_media import rasterize_media_bytes


class RasterizeMediaBytesTest(unittest.TestCase):
    @patch(
        "docreader.parser.pptx_media._rasterize_with_libreoffice",
        return_value=b"png-from-libreoffice",
    )
    @patch(
        "docreader.parser.pptx_media._rasterize_with_imagemagick",
        return_value=None,
    )
    def test_libreoffice_is_fallback_for_emf_without_imagemagick_delegate(
        self, _imagemagick, libreoffice
    ):
        result = rasterize_media_bytes("figure.x-wmf", b"emf-bytes")

        self.assertEqual(result, b"png-from-libreoffice")
        libreoffice.assert_called_once_with(b"emf-bytes", ".x-wmf")


if __name__ == "__main__":
    unittest.main()
