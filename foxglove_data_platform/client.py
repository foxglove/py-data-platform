import datetime
import os
from enum import Enum
from io import BytesIO
import json
from typing import IO, Any, Dict, List, Optional, Union, Callable

import arrow
import requests
from typing_extensions import Protocol


from mcap.records import Schema
from mcap.records import Message
from mcap.reader import make_reader


def _err_on_construction(err):
    def construct():
        raise FoxgloveException(f"Error importing decoder implementation: {err}")

    return construct


try:
    from mcap_ros1.decoder import Decoder as Ros1Decoder
except ModuleNotFoundError as err:
    Ros1Decoder = _err_on_construction(err)

try:
    from mcap_protobuf.decoder import Decoder as ProtobufDecoder
except ModuleNotFoundError as err:
    ProtobufDecoder = _err_on_construction(err)

try:
    from mcap_ros2.decoder import Decoder as Ros2Decoder
except ModuleNotFoundError as err:
    Ros2Decoder = _err_on_construction(err)


class JsonDecoder:
    def decode(self, schema_: Schema, message: Message):
        return json.loads(message.data.decode("utf-8"))


def decoder_for_schema_encoding(encoding_string):
    if encoding_string == "ros1msg":
        return Ros1Decoder()
    if encoding_string == "ros2msg":
        return Ros2Decoder()
    if encoding_string == "protobuf":
        return ProtobufDecoder()
    if encoding_string == "jsonschema":
        return JsonDecoder()
    raise FoxgloveException(f"No known decoder class for encoding {encoding_string}")


def camelize(snake_name: Optional[str]) -> Optional[str]:
    """
    Convert a valid snake_case field name to camelCase for the API
    """
    if not snake_name:
        return snake_name
    parts = snake_name.split("_")
    return "".join([parts[0]] + [w.title() for w in parts[1:]])


class FoxgloveException(Exception):
    pass


class ProgressCallback(Protocol):
    def __call__(self, progress: int) -> None:
        pass


class SizeProgressCallback(Protocol):
    def __call__(self, size: int, progress: int) -> None:
        pass


class OutputFormat(Enum):
    bag = "bag1"
    mcap0 = "mcap0"


class ProgressBufferReader(IO[Any]):
    def __init__(
        self,
        buf: Union[bytes, IO[Any]],
        callback: Optional[SizeProgressCallback] = None,
    ):
        self._callback = callback
        self._progress = 0
        if isinstance(buf, bytes):
            self._length = len(buf)
            self._buf = BytesIO(buf)
        else:
            self._length = os.fstat(buf.fileno()).st_size
            self._buf = buf

    def __len__(self):
        return self._length

    def read(self, n: int = -1) -> bytes:
        chunk = self._buf.read(n) or bytes()
        self._progress += int(len(chunk))
        if self._callback:
            self._callback(size=self._length or 0, progress=self._progress)
        return chunk

    def tell(self) -> int:
        return self._progress


def json_or_raise(response: requests.Response):
    """
    Returns parsed JSON response, or raises if API returned an error.
    For client errors (4xx), the server message is included.
    """
    try:
        json = response.json()
    except ValueError:
        raise requests.exceptions.HTTPError(
            "500 Server Error: Unexpected format", response=response
        )

    if 400 <= response.status_code < 500:
        response.reason = json.get("error", response.reason)

    response.raise_for_status()

    return json


class Client:
    """
    A client for interacting with Foxglove Data Platform. See
    https://foxglove.dev/docs/data-platform for documentation and examples.

    :param token: the API token to authenticate against the host with.
    :param host: The domain to interact with.
    :param decoders: an optional dictionary of ``{schema encoding: decoder}``, where a decoder is
        some callable that takes a :class:`mcap.Schema` and :class:`mcap.Message` and returns
        a deserialized message. This is used in :func:`get_messages` to decode messages downloaded
        from Data Platform. If `None`, :func:`get_messages` will attempt to load decoder classes
        from any support packages (such as `mcap_ros1_support`) available on the module search path.
    """

    def __init__(
        self,
        token: str,
        host: str = "api.foxglove.dev",
        decoders: Optional[Dict[str, Callable[[Schema, Message], Any]]] = None,
    ):
        self._token = token
        self._headers = {
            "Content-type": "application/json",
            "Authorization": "Bearer " + self._token,
        }
        self._host = host
        self._decoders: Dict[str, Callable[[Schema, Message], Any]] = {}
        self._auto_load_decoders = False
        if decoders is not None:
            self._decoders = decoders
        else:
            self._auto_load_decoders = True

    def _url(self, path: str):
        return f"https://{self._host}{path}"

    def create_event(
        self,
        device_id: str,
        time: datetime.datetime,
        duration: int,
        metadata: Optional[Dict[str, str]] = {},
    ):
        """
        Creates a new event.

        device_id: The unique of the device associated with this event.
        time: The time at which the event occurred.
        duration: The duration of the event. Zero for an instantaneous event.
        metadata: Optional metadata attached to the event.
        """
        response = requests.post(
            self._url("/beta/device-events"),
            headers=self._headers,
            json={
                "deviceId": device_id,
                "durationNanos": str(duration),
                "metadata": metadata,
                "timestamp": time.astimezone().isoformat(),
            },
        )

        event = json_or_raise(response)
        return {
            "id": event["id"],
            "device_id": event["deviceId"],
            "timestamp_nanos": event["timestampNanos"],
            "duration_nanos": event["durationNanos"],
            "metadata": event["metadata"],
            "created_at": arrow.get(event["createdAt"]).datetime,
            "updated_at": arrow.get(event["updatedAt"]).datetime,
        }

    def delete_event(
        self,
        event_id: str,
    ):
        """
        Deletes an event.

        event_id: The id of the event to delete.
        """
        response = requests.delete(
            self._url(f"/beta/device-events/{event_id}"),
            headers=self._headers,
        )
        json_or_raise(response)

    def get_events(
        self,
        device_id: Optional[str] = None,
        device_name: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        key: Optional[str] = None,
        value: Optional[str] = None,
    ):
        """
        Retrieves events.

        device_id: Id of the device associated with the events.
        device_name: Name of the device associated with events.
            Either device_id or device_name is required.
        sort_by: Optionally sort records by this field name (e.g. "device_id").
        sort_order: Optionally specify the sort order, either "asc" or "desc".
        limit: Optionally limit the number of records return.
        offset: Optionally offset the results by this many records.
        start: Optionally exclude records before this time.
        end: Optionally exclude records after this time.
        key: Optionally only return records having this key = this value.
        value: Optionally only return records having this key = this value.
        """
        if not device_id and not device_name:
            raise FoxgloveException("One of device_id or device_name is required.")

        params = {
            "deviceId": device_id,
            "deviceName": device_name,
            "sortBy": camelize(sort_by),
            "sortOrder": sort_order,
            "limit": limit,
            "offset": offset,
            "start": start.astimezone().isoformat() if start else None,
            "end": end.astimezone().isoformat() if end else None,
            "key": key,
            "value": value,
        }
        response = requests.get(
            self._url("/beta/device-events"),
            headers=self._headers,
            params={k: v for k, v in params.items() if v},
        )

        json = json_or_raise(response)

        return [
            {
                "id": e["id"],
                "device_id": e["deviceId"],
                "duration": int(e["durationNanos"]),
                "metadata": e["metadata"],
                # datetime doesn't support nanoseconds so we have to divide by 1e9 first.
                "timestamp": arrow.get(int(e["timestampNanos"]) / 1e9).datetime,
                "timestamp_nanos": int(e["timestampNanos"]),
                "created_at": arrow.get(e["createdAt"]).datetime,
                "updated_at": arrow.get(e["updatedAt"]).datetime,
            }
            for e in json
        ]

    def get_messages(
        self,
        device_id: str,
        start: datetime.datetime,
        end: datetime.datetime,
        topics: List[str] = [],
    ):
        """
        Returns a list of tuples of (topic, raw mcap record, decoded message).

        device_id: The id of the device that originated the desired data.
        start: The earliest time from which to retrieve data.
        end: The latest time from which to retrieve data.
        topics: An optional list of topics to retrieve.
            All topics will be retrieved if this is omitted.
        """
        data = self.download_data(
            device_id=device_id, start=start, end=end, topics=topics
        )
        reader = make_reader(BytesIO(data))
        output_messages = []
        for schema, channel, message in reader.iter_messages():
            if schema.encoding not in self._decoders:
                if self._auto_load_decoders:
                    decoder_instance = decoder_for_schema_encoding(schema.encoding)
                    decoder = decoder_instance.decode
                    self._decoders[schema.encoding] = decoder
                else:
                    raise FoxgloveException(
                        f"no decoder provided for schema encoding {schema.encoding}"
                    )
            else:
                decoder = self._decoders[schema.encoding]
            output_messages.append((channel.topic, message, decoder(schema, message)))
        return output_messages

    def download_data(
        self,
        device_id: str,
        start: datetime.datetime,
        end: datetime.datetime,
        topics: List[str] = [],
        output_format: OutputFormat = OutputFormat.mcap0,
        callback: Optional[ProgressCallback] = None,
    ) -> bytes:
        """
        Returns raw data bytes.

        device_id: The id of the device that originated the desired data.
        start: The earliest time from which to retrieve data.
        end: The latest time from which to retrieve data.
        topics: An optional list of topics to retrieve.
            All topics will be retrieved if this is omitted.
        output_format: The output format of the data, either .bag or .mcap, defaulting to .mcap.
        """
        params = {
            "deviceId": device_id,
            "end": end.astimezone().isoformat(),
            "outputFormat": output_format.value,
            "start": start.astimezone().isoformat(),
            "topics": topics,
        }
        link_response = requests.post(
            self._url("/v1/data/stream"),
            headers=self._headers,
            json={k: v for k, v in params.items() if v},
        )

        json = json_or_raise(link_response)

        link = json["link"]
        response = requests.get(link, stream=True)
        data = BytesIO()
        for chunk in response.iter_content(chunk_size=32 * 1024):
            data.write(chunk)
            if callback:
                callback(progress=data.tell())
        return data.getvalue()

    def get_coverage(
        self,
        start: datetime.datetime,
        end: datetime.datetime,
        device_id: Optional[str] = None,
        tolerance: Optional[int] = None,
    ):
        """
        List coverage ranges for data.

        :param start: The earliest time after which to retrieve data.
        :param end: The latest time before which to retrieve data.
        :param device_id: Optional device id to limit data by.
        :param tolerance: Minimum interval (in seconds) that ranges must be separated by
            to be considered discrete.
        """
        params = {
            "deviceId": device_id,
            "tolerance": tolerance,
            "start": start.astimezone().isoformat(),
            "end": end.astimezone().isoformat(),
        }
        response = requests.get(
            self._url("/v1/data/coverage"),
            headers=self._headers,
            params={k: v for k, v in params.items() if v},
        )
        json = json_or_raise(response)

        return [
            {
                "device_id": c["deviceId"],
                "start": arrow.get(c["start"]).datetime,
                "end": arrow.get(c["end"]).datetime,
            }
            for c in json
        ]

    def get_device(self, device_id: str):
        """
        Gets a single device by id.

        :param device_id: The id of the device to retrieve.
        """
        response = requests.get(
            self._url(f"/v1/devices/{device_id}"),
            headers=self._headers,
        )

        device = json_or_raise(response)

        return {
            "id": device["id"],
            "name": device["name"],
            "serial_number": device["serialNumber"],
            "created_at": arrow.get(device["createdAt"]).datetime,
            "updated_at": arrow.get(device["updatedAt"]).datetime,
        }

    def get_devices(self):
        """
        Returns a list of all devices.
        """
        response = requests.get(
            self._url("/v1/devices"),
            headers=self._headers,
        )

        json = json_or_raise(response)

        return [
            {
                "id": d["id"],
                "name": d["name"],
                "serial_number": d["serialNumber"],
                "created_at": arrow.get(d["createdAt"]).datetime,
                "updated_at": arrow.get(d["updatedAt"]).datetime,
            }
            for d in json
        ]

    def create_device(
        self,
        name: str,
        serial_number: str,
    ):
        """
        Creates a new device.

        device_id: The name of the devicee.
        serial_number: The unique serial number of the devicde.
        """
        response = requests.post(
            self._url("/v1/devices"),
            headers=self._headers,
            json={
                "name": name,
                "serialNumber": serial_number,
            },
        )

        device = json_or_raise(response)

        return {
            "id": device["id"],
            "name": device["name"],
            "serial_number": device["serialNumber"],
        }

    def delete_device(self, device_id: str):
        """
        Deletes an existing device.

        Note: you must first delete all imports from the device; see `delete_import`.

        :param device_id: The id of the device.
        """
        response = requests.delete(
            self._url(f"/v1/devices/{device_id}"),
            headers=self._headers,
        )
        json_or_raise(response)

    def delete_import(self, device_id: str, import_id: str):
        """
        Deletes an existing import.

        :param device_id: The id of the device associated with the import.
        :param import_id: The id of the import to delete.
        """
        response = requests.delete(
            self._url(f"/v1/data/imports/{import_id}"),
            params={"deviceId": device_id},
            headers=self._headers,
        )
        json_or_raise(response)

    def get_imports(
        self,
        device_id: Optional[str] = None,
        start: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        data_start: Optional[datetime.datetime] = None,
        data_end: Optional[datetime.datetime] = None,
        include_deleted: bool = False,
        filename: Optional[str] = None,
    ):
        """
        Fetches imports.

        :param device_id: The id of the device associated with the import.
        :param start: Optionally filter by import start time.
        :param end: Optionally filter by import end time.
        :param data_start: Optionally filter by data start time.
        :param data_end: Optionally filter by data end time.
        :param include_deleted: Include deleted imports.
        :param filename: Optionally filter by matching filename.
        """
        all_params = {
            "deviceId": device_id,
            "start": start.astimezone().isoformat() if start else None,
            "end": end.astimezone().isoformat() if end else None,
            "dataStart": data_start.astimezone().isoformat() if data_start else None,
            "dataEnd": data_end.astimezone().isoformat() if data_end else None,
            "includeDeleted": include_deleted,
            "filename": filename,
        }
        response = requests.get(
            self._url("/v1/data/imports"),
            params={k: v for k, v in all_params.items() if v},
            headers=self._headers,
        )
        json = json_or_raise(response)

        return [
            {
                "import_id": i["importId"],
                "device_id": i["deviceId"],
                "import_time": arrow.get(i["importTime"]).datetime,
                "start": arrow.get(i["start"]).datetime,
                "end": arrow.get(i["end"]).datetime,
                "metadata": i["metadata"],
                "input_type": i["inputType"],
                "output_type": i["outputType"],
                "filename": i["filename"],
                "input_size": i["inputSize"],
                "total_output_size": i["totalOutputSize"],
            }
            for i in json
        ]

    def get_topics(
        self,
        device_id: str,
        start: datetime.datetime,
        end: datetime.datetime,
    ):
        response = requests.get(
            self._url("/v1/data/topics"),
            headers=self._headers,
            params={
                "deviceId": device_id,
                "start": start.astimezone().isoformat(),
                "end": end.astimezone().isoformat(),
                "includeSchemas": "false",
            },
        )

        json = json_or_raise(response)

        return [
            {
                "topic": t["topic"],
                "version": t["version"],
                "encoding": t["encoding"],
                "schema_encoding": t["schemaEncoding"],
                "schema_name": t["schemaName"],
            }
            for t in json
        ]

    def upload_data(
        self,
        device_id: str,
        filename: str,
        data: Union[bytes, IO[Any]],
        callback: Optional[SizeProgressCallback] = None,
    ):
        """
        Uploads data in bytes.

        device_id: Device id of the device from which this data originated.
        filename: A filename to associate with the data. The data format will be
            inferred from the file extension.
        data: The raw data in .bag or .mcap format.
        callback: An optional callback to report progress on the upload.
        """
        link_response = requests.post(
            self._url("/v1/data/upload"),
            headers=self._headers,
            json={
                "deviceId": device_id,
                "filename": filename,
            },
        )

        json = json_or_raise(link_response)

        link = json["link"]
        buffer = ProgressBufferReader(data, callback=callback)
        upload_request = requests.put(
            link,
            data=buffer,
            headers={"Content-Type": "application/octet-stream"},
        )
        return {
            "link": link,
            "text": upload_request.text,
            "code": upload_request.status_code,
        }


__all__ = ["Client", "FoxgloveException", "OutputFormat"]
