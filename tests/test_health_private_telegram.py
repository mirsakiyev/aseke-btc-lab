import json

import httpx
import pytest

from lab.health import delivery, private_telegram, state


def test_private_route_and_message_are_bound_to_same_destination(monkeypatch):
    monkeypatch.setattr(delivery, "credentials", lambda: ("123:fixture_token", "12345"))
    calls = []

    def serve(request):
        body = json.loads(request.content)
        calls.append((request.url.path.rsplit("/", 1)[-1], body))
        chat = {"id": 12345, "type": "private"}
        response = chat if len(calls) == 1 else {"chat": chat, "message_id": 3, "date": 42}
        return httpx.Response(200, json={"ok": True, "result": response})

    transport = httpx.MockTransport(serve)
    route = private_telegram.destination(transport=transport)
    result = private_telegram.send(route, private_telegram.CONNECTIVITY_TEXT, transport=transport)
    assert result.state == "DELIVERED" and result.api_accepted_at is not None
    assert calls[0] == ("getChat", {"chat_id": 12345})
    assert calls[1][1]["text"] == private_telegram.CONNECTIVITY_TEXT
    assert calls[1][1]["protect_content"] is True
    assert "fixture_token" not in repr(route) and "12345" not in repr(route)


@pytest.mark.parametrize("reported", [{"id": 12345, "type": "group"}, {"id": 12346, "type": "private"}])
def test_group_or_different_destination_fails_closed(monkeypatch, reported):
    monkeypatch.setattr(delivery, "credentials", lambda: ("123:fixture_token", 12345))
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json={"ok": True, "result": reported}))
    with pytest.raises(PermissionError, match="exact private chat"):
        private_telegram.destination(transport=transport)


def test_existing_outbox_cannot_send_private_rows_through_legacy_path(tmp_path, monkeypatch):
    monkeypatch.setattr(state, "ROOT", tmp_path)
    sends = []

    def sender(kind):
        def send(text):
            sends.append((kind, text))
            return delivery.Delivery("DELIVERED", "accepted")
        return send

    with state.connect() as db:
        state.enqueue(db, "phase15-event", "paper_entry", "legacy", 1)
        state.enqueue(db, "ema-private:fill", "paper_entry", "summary", 1,
                      event_json=json.dumps({"private_route_id": "authorized"}))
        db.commit()
        state.flush(db, 2, state.policy(), sender("legacy"))
        state.flush(db, 3, state.policy(), sender("legacy"), private_route_id="wrong", private_send=sender("private"))
        assert sends == [("legacy", "legacy")]
        assert db.execute("SELECT attempts FROM outbox WHERE id='ema-private:fill'").fetchone()[0] == 0
        for now in (4, 5):
            state.flush(db, now, state.policy(), sender("legacy"), private_route_id="authorized", private_send=sender("private"))
        assert sends == [("legacy", "legacy"), ("private", "summary")]
