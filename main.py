from fastapi import FastAPI, File, UploadFile, Response, status
from fastapi.responses import HTMLResponse, FileResponse
from datetime import datetime, timedelta
from rethinkdb import RethinkDB
from rethinkdb.errors import ReqlOpFailedError, ReqlDriverError, ReqlError
import os, uuid, aiofiles, jobscheduler, shutil, logging, sys

#Configure logging
drop_logger = logging.getLogger("drop")
drop_logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = formatter = logging.Formatter('[%(levelname)s] - %(asctime)s - %(message)s', "%Y-%m-%d %H:%M:%S")
ch.setFormatter(formatter)
drop_logger.addHandler(ch)

#Initialize application stuff.
scheduler = jobscheduler.JobScheduler(logging=True, in_memory=False)
app = FastAPI()
client = RethinkDB()

#Database variables
DB_HOST = "localhost"
DB_PORT = 28015
DB_NAME = "drop"
DB_TABLE = "files"

#Making sure the correct database and table exist.
try:
    conn = client.connect(host=DB_HOST, port=DB_PORT, db=DB_NAME).repl()
    client.db_create(DB_NAME).run()
    #If db_create executed successfully, that means the database is new, therefore create the table as well.
    client.table_create(DB_TABLE).run()
except ReqlOpFailedError:
    drop_logger.info("Drop database already created, skipping...");
    pass 
except ReqlDriverError:
    #Meaning the client failed to connect, exit.
    drop_logger.critical("Failed to connect to rethinkdb! Exiting...");
    sys.exit()

if not os.path.exists("./uploaded_files"):
    os.mkdir("./uploaded_files")

def delete_directory(path, file_id):
    shutil.rmtree(path)
    try:
        client.table(DB_TABLE).filter(client.row["file_id"] == file_id).delete().run(conn)
    except ReqlError as e:
        print(e)
        drop_logger.error(f"Error deleting row with the file_id of {file_id}")

@app.post("/uploadfile/")
async def upload_file(response: Response, file: UploadFile = File(...)):
    try:
        new_uuid = str(uuid.uuid4())
        os.mkdir(dirPath := os.path.join("./uploaded_files", new_uuid))
        try:
            async with aiofiles.open(fullFilePath := os.path.join(dirPath, file.filename), "wb") as out_file:
                data = await file.read()
                await out_file.write(data)
        except IOError:
            #When there is no disk space left.
            response.status_code = status.HTTP_507_INSUFFICIENT_STORAGE
            return {"success" : False, "error" : "Service too busy (storage full), try again later."}
        await out_file.close()
        client.table(DB_TABLE).insert({"file_id": new_uuid, "file": fullFilePath}).run()
        time_to_delete = datetime.now() + timedelta(minutes=1)
        #We want to delete the whole directory where the file is stored, not just the file on its own.
        scheduler.schedule_job(delete_directory, [dirPath, new_uuid], time_to_delete, f"Delete {fullFilePath}.")
        response.status_code = status.HTTP_200_OK
        return {"success": True}
    except Exception as e:
        drop_logger.error(e)
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"success" : False}

@app.get("/file/{file_id}")
async def get_file(response: Response, file_id: str):
    document = client.table(DB_TABLE).filter(client.row["file_id"] == file_id).run()
    try:
        file_path = [x for x in document][0]["file"]
    except IndexError:
        #Meaning there is no document with that id.
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"success": False, "error": "File not found or expired!"}
    response.status_code = status.HTTP_200_OK
    return FileResponse(file_path)
