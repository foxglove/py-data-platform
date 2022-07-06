import time
from datetime import datetime

import responses
from faker import Faker
from foxglove_data_platform.client import Client

from .api_url import api_url

fake = Faker()


@responses.activate
def test_create_event():
    id = fake.uuid4()
    device_id = fake.uuid4()
    device_name = "my_device_name"
    responses.add(
        responses.POST,
        api_url("/beta/device-events"),
        json={
            "id": id,
            "deviceId": device_id,
            "deviceName": device_name,
            "timestampNanos": str(time.time_ns()),
            "durationNanos": "1",
            "metadata": {"foo": "bar"},
            "createdAt": datetime.now().isoformat(),
            "updatedAt": datetime.now().isoformat(),
        },
    )
    client = Client("test")
    event = client.create_event(
        device_id=device_id, device_name=device_name, time=datetime.now(), duration=1
    )
    assert event["id"] == id
    assert event["device_id"] == device_id


@responses.activate
def test_delete_event():
    id = fake.uuid4()
    responses.add(
        responses.DELETE,
        api_url(f"/beta/device-events/{id}"),
    )
    client = Client("test")
    try:
        client.delete_event(event_id=id)
    except:
        assert False


@responses.activate
def test_get_events():
    device_id = "my_device_id"
    responses.add(
        responses.GET,
        api_url(f"/beta/device-events?deviceId={device_id}"),
        json=[
            {
                "id": "1",
                "createdAt": datetime.now().isoformat(),
                "deviceId": device_id,
                "durationNanos": fake.pyint(),
                "metadata": {},
                "timestampNanos": fake.pyint(),
                "updatedAt": datetime.now().isoformat(),
            }
        ],
    )
    client = Client("test")
    events = client.get_events(device_id=device_id)
    assert len(events) == 1
    assert events[0]["device_id"] == device_id
