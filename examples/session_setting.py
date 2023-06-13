from databend_py import Client


def session_settings():
    # docs: https://databend.rs/doc/integrations/api/rest#client-side-session
    session_settings = {"db": "test"}
    client = Client(host="localhost", port=8000, user="root", password="root", session_settings=session_settings)
    print(client)
