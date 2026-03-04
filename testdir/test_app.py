# assumes the existence of app.py and paramdump.json in the same dir
from fastapi.testclient import TestClient
from app import app

client = TestClient(app)


def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "post to /notebook"}


def test_notebook_does_not_exist():
    response = client.post("/notebook/does_not_exist")
    assert response.status_code == 421
    assert response.json()["detail"] == "notebook does not exist"


def test_notebook_success_no_params():
    response = client.post("/notebook/LiDAR_Vlab_tutorial.ipynb")
    assert response.status_code == 202
    assert "execution_id" in response.json()


def test_notebook_success_with_valid_params():
    response = client.post(
        "/notebook/LiDAR_Vlab_tutorial.ipynb",
        json={
            "param_minio_endpoint": "scruffy.lab.uvalight.net:9000",
        }
    )
    assert response.status_code == 202
    assert "execution_id" in response.json()


def test_notebook_invalid_param():
    response = client.post(
        "/notebook/LiDAR_Vlab_tutorial.ipynb",
        json={
            "not_a_real_param": 999
        }
    )
    assert response.status_code == 400
    assert "does not exist" in response.json()["detail"]
