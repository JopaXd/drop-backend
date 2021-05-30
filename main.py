from fastapi import FastAPI, File, UploadFile, Response, status
from fastapi.responses import HTMLResponse
from datetime import datetime, timedelta
import os, uuid, aiofiles, jobscheduler, shutil

scheduler = jobscheduler.JobScheduler(logging=True, in_memory=False)
app = FastAPI()

def delete_directory(path):
    shutil.rmtree(path)

@app.post("/uploadfile/")
async def upload_file(response: Response, file: UploadFile = File(...)):
    try:
        new_uuid = str(uuid.uuid4())
        os.mkdir(dirPath := os.path.join("./uploaded_files", new_uuid))
        async with aiofiles.open(fullFilePath := os.path.join(dirPath, file.filename), "wb") as out_file:
            data = await file.read()
            await out_file.write(data)
        await out_file.close()
        time_to_delete = datetime.now() + timedelta(hours=1)
        #We want to delete the whole directory where the file is stored, not just the file on its own.
        scheduler.schedule_job(delete_directory, [dirPath], time_to_delete, f"Delete {file.filename}.")
        response.status_code = status.HTTP_200_OK
        return {"status": "success!"}
    except Exception as e:
        print(e)
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"status" : "errror!"}
