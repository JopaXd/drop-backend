from typing import Optional
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse
import os
import uuid
import aiofiles

app = FastAPI()

@app.post("/uploadfile/")
async def upload_file(file: UploadFile = File(...)):
    try:
        new_uuid = str(uuid.uuid4())
        os.mkdir(path := os.path.join("./uploaded_files", new_uuid))
        async with aiofiles.open(os.path.join(path, file.filename), "wb") as out_file:
            data = await file.read()
            await out_file.write(data)
        await out_file.close()
        return {"status": "success!"}
    except Exception as e:
        print(e)
        return {"status" : "errror!"}
