import logging
from unittest.mock import Mock

import pytest

from flows import drillhole_raw_landing as drl


@pytest.fixture(autouse=True)
def stub_run_logger(monkeypatch):
    """Tasks/flows call get_run_logger(), which raises outside an
    orchestrated Prefect run. Swap in a plain stdlib logger so unit tests
    don't need a live Prefect server."""
    monkeypatch.setattr(drl, "get_run_logger", lambda: logging.getLogger("test"))


def _fake_response(*, content: bytes, json_data: dict | None = None):
    response = Mock()
    response.content = content
    response.raise_for_status = Mock()
    if json_data is not None:
        response.json = Mock(return_value=json_data)
    return response


class TestSaveRaw:
    def test_local_backend_writes_bytes_untouched(self, tmp_path, monkeypatch):
        monkeypatch.setattr(drl, "RAW_STORAGE_BACKEND", "local")
        monkeypatch.setattr(drl, "RAW_LOCAL_DIR", str(tmp_path))

        content = b'{"features": []}'
        uri = drl.save_raw.fn("wa_dmirs", "20260101T000000Z", "page_00000000.json", content)

        written = tmp_path / "wa_dmirs" / "20260101T000000Z" / "page_00000000.json"
        assert written.read_bytes() == content
        assert uri == str(written)

    def test_local_backend_creates_nested_dirs(self, tmp_path, monkeypatch):
        monkeypatch.setattr(drl, "RAW_STORAGE_BACKEND", "local")
        monkeypatch.setattr(drl, "RAW_LOCAL_DIR", str(tmp_path / "does" / "not" / "exist"))

        drl.save_raw.fn("usgs_mrds", "run1", "mrds-csv.zip", b"PK\x03\x04")

        assert (tmp_path / "does" / "not" / "exist" / "usgs_mrds" / "run1" / "mrds-csv.zip").exists()

    def test_s3_backend_uploads_to_configured_bucket(self, monkeypatch):
        monkeypatch.setattr(drl, "RAW_STORAGE_BACKEND", "s3")
        monkeypatch.setattr(drl, "RAW_S3_BUCKET", "my-bucket")
        monkeypatch.setattr(drl, "RAW_S3_PREFIX", "raw")

        fake_client = Mock()
        monkeypatch.setattr("boto3.client", Mock(return_value=fake_client))

        content = b"zip-bytes"
        uri = drl.save_raw.fn("usgs_mrds", "run1", "mrds-csv.zip", content)

        fake_client.put_object.assert_called_once_with(
            Bucket="my-bucket", Key="raw/usgs_mrds/run1/mrds-csv.zip", Body=content
        )
        assert uri == "s3://my-bucket/raw/usgs_mrds/run1/mrds-csv.zip"

    def test_s3_backend_requires_bucket(self, monkeypatch):
        monkeypatch.setattr(drl, "RAW_STORAGE_BACKEND", "s3")
        monkeypatch.setattr(drl, "RAW_S3_BUCKET", None)

        with pytest.raises(RuntimeError, match="RAW_S3_BUCKET"):
            drl.save_raw.fn("usgs_mrds", "run1", "mrds-csv.zip", b"data")


class TestDownloadWaDrillholesRaw:
    def test_stops_when_transfer_limit_not_exceeded(self, monkeypatch):
        response = _fake_response(
            content=b'{"features": [1, 2], "exceededTransferLimit": false}',
            json_data={"features": [1, 2], "exceededTransferLimit": False},
        )
        monkeypatch.setattr(drl.requests, "get", Mock(return_value=response))
        save_raw_mock = Mock(return_value="saved-uri")
        monkeypatch.setattr(drl, "save_raw", save_raw_mock)

        uris = drl.download_wa_drillholes_raw.fn("run1", None)

        assert uris == ["saved-uri"]
        save_raw_mock.assert_called_once_with("wa_dmirs", "run1", "page_00000000.json", response.content)

    def test_pages_until_transfer_limit_clears(self, monkeypatch):
        page1 = _fake_response(
            content=b"page1", json_data={"features": [1, 2], "exceededTransferLimit": True}
        )
        page2 = _fake_response(
            content=b"page2", json_data={"features": [3, 4], "exceededTransferLimit": True}
        )
        page3 = _fake_response(
            content=b"page3", json_data={"features": [5], "exceededTransferLimit": False}
        )
        get_mock = Mock(side_effect=[page1, page2, page3])
        monkeypatch.setattr(drl.requests, "get", get_mock)
        save_raw_mock = Mock(side_effect=["uri1", "uri2", "uri3"])
        monkeypatch.setattr(drl, "save_raw", save_raw_mock)

        uris = drl.download_wa_drillholes_raw.fn("run1", None)

        assert uris == ["uri1", "uri2", "uri3"]
        assert get_mock.call_count == 3
        offsets = [call.kwargs["params"]["resultOffset"] for call in get_mock.call_args_list]
        assert offsets == [0, 2, 4]

    def test_max_records_caps_pagination(self, monkeypatch):
        page = _fake_response(
            content=b"page", json_data={"features": [1, 2], "exceededTransferLimit": True}
        )
        get_mock = Mock(return_value=page)
        monkeypatch.setattr(drl.requests, "get", get_mock)
        monkeypatch.setattr(drl, "save_raw", Mock(return_value="uri"))

        # Every page reports exceededTransferLimit=True, so without a cap
        # this would loop forever; max_records must break it after one page.
        uris = drl.download_wa_drillholes_raw.fn("run1", 2)

        assert uris == ["uri"]
        assert get_mock.call_count == 1

    def test_stops_on_empty_features(self, monkeypatch):
        empty = _fake_response(
            content=b"empty", json_data={"features": [], "exceededTransferLimit": True}
        )
        monkeypatch.setattr(drl.requests, "get", Mock(return_value=empty))
        monkeypatch.setattr(drl, "save_raw", Mock(return_value="uri"))

        uris = drl.download_wa_drillholes_raw.fn("run1", None)

        assert uris == ["uri"]


class TestDownloadMrdsRaw:
    def test_downloads_and_saves_zip_untouched(self, monkeypatch):
        content = b"PK\x03\x04-fake-zip-bytes"
        response = _fake_response(content=content)
        monkeypatch.setattr(drl.requests, "get", Mock(return_value=response))
        save_raw_mock = Mock(return_value="local://mrds-csv.zip")
        monkeypatch.setattr(drl, "save_raw", save_raw_mock)

        uri = drl.download_mrds_raw.fn("run1")

        save_raw_mock.assert_called_once_with("usgs_mrds", "run1", "mrds-csv.zip", content)
        assert uri == "local://mrds-csv.zip"

    def test_raises_on_http_error(self, monkeypatch):
        response = Mock()
        response.raise_for_status = Mock(side_effect=drl.requests.exceptions.HTTPError("500"))
        monkeypatch.setattr(drl.requests, "get", Mock(return_value=response))

        with pytest.raises(drl.requests.exceptions.HTTPError):
            drl.download_mrds_raw.fn("run1")


class TestDrillholeRawLandingFlow:
    def test_calls_both_downloads_with_shared_run_stamp(self, monkeypatch):
        wa_mock = Mock(return_value=["wa-uri"])
        mrds_mock = Mock(return_value="mrds-uri")
        monkeypatch.setattr(drl, "download_wa_drillholes_raw", wa_mock)
        monkeypatch.setattr(drl, "download_mrds_raw", mrds_mock)

        drl.drillhole_raw_landing.fn(wa_max_records=100)

        wa_mock.assert_called_once()
        mrds_mock.assert_called_once()
        wa_run_stamp = wa_mock.call_args.args[0]
        mrds_run_stamp = mrds_mock.call_args.args[0]
        assert wa_run_stamp == mrds_run_stamp
        assert wa_mock.call_args.args[1] == 100
