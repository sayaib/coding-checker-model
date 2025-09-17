from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import uvicorn
from loguru import logger
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

from .project.prepare_data_model import *
from .project.rule_checker import *
from .project.azure_storage import AzureBlobStorageManager
import time
import pandas as pd
import os
from fastapi import Request
from pydantic import BaseModel
from typing import *
from .project.model import *
import tempfile
import shutil

# =======================  Define Logger Code Here ====================
logger.add(
    "execute_rule_data_modelling.log",
    format="{time} | {level} | {message} | {name} | {file} | line {line}",
    level="TRACE",
    rotation="100 MB",
)

############Data Models ###########


#######################################
## Function Maps
function_map = {
    "1": rule1,
    "2": rule2,
    "3": rule3,
    "4_1": rule4_1,
    "4_2": rule4_2,
    "4_3": rule4_3,
    "10": rule10,
    "11": rule11,
    "12": rule12,
    "14": rule14,
    "15": rule15,
    "16": rule16,
    "18": rule18,
    "19": rule19,
    "20": rule20,
    "24": rule24,
    "25": rule25,
    "26": rule26,
    "27": rule27,
    "33": rule33,
    "34": rule34,
    "35": rule35,
    "37": rule37,
    "40": rule40,
    "45": rule45,
    "46": rule46,
    "47": rule47,
    "48": rule48,
    "49": rule49,
    "50": rule50,
    "51": rule51,
    "55": rule55,
    "56": rule56,
    "62": rule62,
    "63": rule63,
    "67": rule67,
    "70": rule70,
    "71": rule71,
    "76": rule76,
    "80": rule80,
    "90": rule90,
    "93": rule93,
    "94": rule94,
    "100": rule100,
}


###################################33

app = FastAPI(docs_url="/testing", redoc_url=None)

# Add CORS middleware to allow frontend connections
# Get CORS settings from environment variables
cors_origins = os.getenv("CORS_ORIGINS", "*").split(",")
cors_credentials = os.getenv("CORS_ALLOW_CREDENTIALS", "true").lower() == "true"
cors_methods = os.getenv("CORS_ALLOW_METHODS", "*").split(",")
cors_headers = os.getenv("CORS_ALLOW_HEADERS", "*").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=cors_credentials,
    allow_methods=cors_methods,
    allow_headers=cors_headers,
)

# Initialize Azure Blob Storage Manager
storage_manager = AzureBlobStorageManager()


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        disconnected_connections = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.warning(f"Failed to send message to WebSocket connection: {e}")
                disconnected_connections.append(connection)

        # Remove disconnected connections
        for connection in disconnected_connections:
            self.active_connections.remove(connection)

    async def broadcast_data_modelling(self, input_dir):
        # Check if input_dir is a local path or blob storage folder name
        input_path = Path(input_dir)

        if input_path.exists() and input_path.is_dir():
            # Input is a local directory, process files directly
            print("Processing local directory:", input_dir)
            await self.broadcast("Processing files from local directory...")

            # Step 1: Get the only XML file from local directory
            pdf_files = list(input_path.glob("*.pdf"))
            xml_files = list(input_path.glob("*.xml"))

            print("api called..")
            print(pdf_files, xml_files)

            if not xml_files or not pdf_files:
                await self.broadcast("No XML or PDF file found.")
                return {
                    "status": "Data Modelling Failed",
                    "reason": "No XML or PDF file found.",
                    "data_model_dir": "",
                }

            pdf_name = os.path.basename(pdf_files[0]).split(".")[0]
            xml_name = os.path.basename(xml_files[0]).split(".")[0]

            if pdf_name != xml_name:
                await self.broadcast("PDF and XML file names do not match")
                return {
                    "status": "Data Modelling Failed",
                    "reason": "PDF and XML file names do not match",
                    "data_model_dir": "",
                }

            xml_file_path = xml_files[0]  # This is a Path object
            dest_file_name = xml_file_path.stem  # filename without extension

            # Step 2: Create output directory for processing in local case
            output_dir = input_path / dest_file_name
            output_dir.mkdir(parents=True, exist_ok=True)

            # Process the files directly from local directory
            result = await self._process_data_modelling(
                xml_file_path, dest_file_name, output_dir, input_path
            )
            
            # Broadcast the final completion response
            await self.broadcast(f"Final Result: {result}")
            return result

        else:
            # Input is a blob storage folder name, check if already exists in model_files
            model_files_dir = Path("model_files")
            model_files_dir.mkdir(exist_ok=True)  # Ensure model_files directory exists

            local_model_path = model_files_dir / input_dir

            if local_model_path.exists() and local_model_path.is_dir():
                # Folder already exists locally, use it directly
                await self.broadcast(
                    f"Using existing folder from model_files: {input_dir}"
                )

                # Check for XML and PDF files in existing folder
                pdf_files = list(local_model_path.glob("*.pdf"))
                xml_files = list(local_model_path.glob("*.xml"))

                if not xml_files or not pdf_files:
                    await self.broadcast("No XML or PDF file found in existing folder.")
                    return {
                        "status": "Data Modelling Failed",
                        "reason": "No XML or PDF file found in existing folder.",
                        "data_model_dir": "",
                    }

                pdf_name = os.path.basename(pdf_files[0]).split(".")[0]
                xml_name = os.path.basename(xml_files[0]).split(".")[0]

                if pdf_name != xml_name:
                    await self.broadcast(
                        "PDF and XML file names do not match in existing folder"
                    )
                    return {
                        "status": "Data Modelling Failed",
                        "reason": "PDF and XML file names do not match in existing folder",
                        "data_model_dir": "",
                    }

                xml_file_path = xml_files[0]
                dest_file_name = xml_file_path.stem

                # Create output directory for processing
                output_dir = local_model_path / dest_file_name
                output_dir.mkdir(parents=True, exist_ok=True)

                # Process the files from existing local directory
                result = await self._process_data_modelling(
                    xml_file_path, dest_file_name, output_dir, local_model_path
                )
                
                # Broadcast the final completion response
                await self.broadcast(f"Final Result: {result}")
                return result

            else:
                # Folder doesn't exist locally, validate contents before downloading
                await self.broadcast(
                    f"Validating folder contents for '{input_dir}' before downloading..."
                )

                try:
                    # Validate folder contents in Azure Blob Storage before downloading
                    validation_result = await storage_manager.validate_folder_contents(
                        input_dir
                    )

                    if not validation_result["valid"]:
                        await self.broadcast(validation_result["message"])
                        return {
                            "status": "Data Modelling Failed",
                            "reason": validation_result["message"],
                            "data_model_dir": "",
                        }

                    # Folder is valid, proceed with download
                    await self.broadcast(
                        f"Folder validation successful. Downloading {input_dir} to model_files directory..."
                    )

                    # Create progress callback for WebSocket updates
                    async def download_progress_callback(progress_data):
                        if isinstance(progress_data, dict):
                            progress_type = progress_data.get('type')
                            
                            if progress_type == 'file_start':
                                await self.broadcast(
                                    f"Starting download: {progress_data['current_file']} ({progress_data['file_index']}/{progress_data['total_files']})"
                                )
                            elif progress_type == 'file_progress':
                                # Calculate percentages
                                file_percent = (progress_data['file_progress'] / progress_data['file_total']) * 100 if progress_data['file_total'] > 0 else 0
                                overall_percent = (progress_data['overall_progress'] / progress_data['overall_total']) * 100 if progress_data['overall_total'] > 0 else 0
                                
                                # Format file size
                                def format_bytes(bytes_val):
                                    for unit in ['B', 'KB', 'MB', 'GB']:
                                        if bytes_val < 1024.0:
                                            return f"{bytes_val:.1f} {unit}"
                                        bytes_val /= 1024.0
                                    return f"{bytes_val:.1f} TB"
                                
                                await self.broadcast(
                                    f"Downloading {progress_data['current_file']}: {file_percent:.1f}% "
                                    f"({format_bytes(progress_data['file_progress'])}/{format_bytes(progress_data['file_total'])}) | "
                                    f"Overall: {overall_percent:.1f}% ({progress_data['files_completed']}/{progress_data['total_files']} files)"
                                )
                            elif progress_type == 'file_complete':
                                await self.broadcast(
                                    f"Completed: {progress_data['current_file']} ({progress_data['files_completed']}/{progress_data['total_files']} files done)"
                                )
                            elif progress_type == 'download_complete':
                                if progress_data['success']:
                                    await self.broadcast(
                                        f"Download completed successfully! All {progress_data['total_files']} files downloaded."
                                    )
                                else:
                                    await self.broadcast(
                                        f"Download completed with issues. {progress_data['files_completed']}/{progress_data['total_files']} files downloaded."
                                    )

                    # Download files from Azure Blob Storage directly to model_files
                    download_success = await storage_manager.download_directory(
                        input_dir, str(local_model_path), download_progress_callback
                    )

                    if not download_success:
                        await self.broadcast(
                            f"Failed to download folder '{input_dir}' from Azure Blob Storage"
                        )
                        return {
                            "status": "Data Modelling Failed",
                            "reason": f"Failed to download folder '{input_dir}' from Azure Blob Storage",
                            "data_model_dir": "",
                        }

                    await self.broadcast(
                        f"Successfully downloaded folder '{input_dir}'. Processing files..."
                    )

                    # Check for XML and PDF files in downloaded folder (double-check after download)
                    pdf_files = list(local_model_path.glob("*.pdf"))
                    xml_files = list(local_model_path.glob("*.xml"))

                    if not xml_files or not pdf_files:
                        await self.broadcast(
                            "No XML or PDF file found in downloaded folder."
                        )
                        return {
                            "status": "Data Modelling Failed",
                            "reason": "No XML or PDF file found in downloaded folder.",
                            "data_model_dir": "",
                        }

                    pdf_name = os.path.basename(pdf_files[0]).split(".")[0]
                    xml_name = os.path.basename(xml_files[0]).split(".")[0]

                    if pdf_name != xml_name:
                        await self.broadcast(
                            "PDF and XML file names do not match in downloaded folder"
                        )
                        return {
                            "status": "Data Modelling Failed",
                            "reason": "PDF and XML file names do not match in downloaded folder",
                            "data_model_dir": "",
                        }

                    xml_file_path = xml_files[0]
                    dest_file_name = xml_file_path.stem

                    # Create output directory for processing
                    output_dir = local_model_path / dest_file_name
                    output_dir.mkdir(parents=True, exist_ok=True)

                    # Process the files from downloaded directory
                    result = await self._process_data_modelling(
                        xml_file_path, dest_file_name, output_dir, local_model_path
                    )
                    
                    # Broadcast the final completion response
                    await self.broadcast(f"Final Result: {result}")
                    return result

                except Exception as e:
                    await self.broadcast(
                        f"Error during folder validation or download: {str(e)}"
                    )
                    return {
                        "status": "Data Modelling Failed",
                        "reason": f"Error during folder validation or download: {str(e)}",
                        "data_model_dir": "",
                    }

    async def _process_data_modelling(
        self, xml_file_path, dest_file_name, output_dir, source_dir
    ):
        """Helper method to process data modelling for both local and blob storage cases"""
        try:
            # Step 3: Call ingest_file in background thread
            await self.broadcast("Extracting ingest_file: 2%")
            result_from_ingest = await asyncio.to_thread(
                ingest_file,
                str(xml_file_path),  # convert Path to string if needed
            )

            all_program_function_names = []

            # Step 4: data_modelling_program_wise
            await self.broadcast("Extracting data_modelling_program_wise: 10%")
            program_names = await asyncio.to_thread(
                data_modelling_program_wise,
                result_from_ingest,
                output_dir,
                f"{dest_file_name}_programwise.csv",
            )

            # Step 5: data_modelling_function_wise
            await self.broadcast("Extracting data_modelling_function_wise: 40%")
            function_names = await asyncio.to_thread(
                data_modelling_function_wise,
                result_from_ingest,
                output_dir,
                f"{dest_file_name}_functionwise.csv",
            )

            # Step 6: extract_variable_comment_programwise
            await self.broadcast("Extracting extract_variable_comment_programwise: 70%")
            await asyncio.to_thread(
                extract_variable_comment_programwise,
                result_from_ingest,
                f"{dest_file_name}_programwise.json",
                output_dir,
            )

            # Step 7: extract_variable_comment_functionwise
            await self.broadcast(
                "Extracting extract_variable_comment_functionwise: 85%"
            )
            await asyncio.to_thread(
                extract_variable_comment_functionwise,
                result_from_ingest,
                f"{dest_file_name}_functionwise.json",
                output_dir,
            )

            # Step 8: Upload results back to Azure Blob Storage
            await self.broadcast("Uploading results to Azure Blob Storage: 90%")
            blob_output_path = f"{dest_file_name}"
            await asyncio.to_thread(
                storage_manager.upload_directory, str(output_dir), blob_output_path
            )

            # Step 9: Upload source files to input-files container with timestamp
            await self.broadcast("Uploading source files to input-files container: 95%")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            input_files_folder = f"{dest_file_name}_{timestamp}"

            try:
                await asyncio.to_thread(
                    storage_manager.upload_directory,
                    str(source_dir),
                    input_files_folder,
                    "input-files",  # container name
                )
                await self.broadcast(
                    f"Source files uploaded to input-files/{input_files_folder}"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to upload source files to input-files container: {e}"
                )
                await self.broadcast(
                    "Warning: Failed to upload source files to input-files container"
                )

            all_program_function_names.extend(program_names)
            all_program_function_names.extend(function_names)

            # Final message
            await self.broadcast("✅ Data Modelling Completed: 100%")

            return {
                "status": "Data Modelling Success",
                "data_model_dir": blob_output_path,
                "all_task_names": all_program_function_names,
            }
        except Exception as e:
            await self.broadcast(f"Error during data modelling: {str(e)}")
            return {
                "status": "Data Modelling Failed",
                "reason": f"Error during processing: {str(e)}",
                "data_model_dir": "",
                "all_task_names": [],
            }

    async def broadcast_rule_checker(
        self, data_model_input_path, input_list, input_image=None
    ):
        # Initialize variables at the beginning
        in_list = input_list
        columns = [
            "Result",
            "Task",
            "Section",
            "RungNo",
            "Target",
            "CheckItem",
            "Detail",
            "Status",
        ]
        standard_df = pd.DataFrame(columns=columns)
        output_df = pd.DataFrame(columns=columns)

        try:
            # Use local files from model_files directory instead of downloading
            await self.broadcast("Loading data model files from local directory...")

            # Use the local files directly from model_files directory
            data_model_local_path = Path(data_model_input_path)
            dest_file_name = data_model_local_path.name  # Get the folder name

            # Create output directory in the project root
            output_dir = Path("output_db")
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file_path = output_dir / f"{dest_file_name}.csv"

            # Find required files in the local directory
            program_file_csv = [
                f
                for f in data_model_local_path.glob("*_programwise.csv")
                if "datasource" not in f.name
            ][0]
            function_file_csv = [
                f
                for f in data_model_local_path.glob("*_functionwise.csv")
                if "datasource" not in f.name
            ][0]
            program_comment_file = list(
                data_model_local_path.glob("*_programwise.json")
            )[0]
            function_comment_file = list(
                data_model_local_path.glob("*_functionwise.json")
            )[0]
            datasource_program_file = list(
                data_model_local_path.glob("*datasource_comments_programwise.csv")
            )[0]
            datasource_function_file = list(
                data_model_local_path.glob("*datasource_comments_functionwise.csv")
            )[0]

            total_rules = len(in_list)
            rule_status = {}
            all_outputs = []

            for idx, rl in enumerate(in_list, start=1):
                function = function_map.get(rl)

                percent_complete = int((idx / total_rules) * 100)
                await self.broadcast(
                    f"Executing Rule {rl} — {percent_complete}% complete"
                )
                if function:
                    function_status = await asyncio.to_thread(
                        function,
                        program_file_csv,
                        program_comment_file,
                        datasource_program_file,
                        function_file_csv,
                        function_comment_file,
                        datasource_function_file,
                        input_image,
                    )

                    if (
                        function_status.get("status") == "SUCCESS"
                        and isinstance(function_status.get("output_df"), pd.DataFrame)
                        and not function_status["output_df"].empty
                    ):
                        output_df = function_status["output_df"]
                        output_df = output_df[standard_df.columns]
                        # output_df.to_csv(
                        #     output_file_path,
                        #     mode="a",
                        #     index=False,
                        #     header=not output_file_path.exists(),
                        # )
                        all_outputs.append(output_df)
                        rule_status[rl] = "SUCCESS"

                    else:
                        if isinstance(function_status.get("output_df"), pd.DataFrame):
                            function_status["output_df"] = function_status[
                                "output_df"
                            ].to_dict(orient="records")
                        if (
                            "output_df" not in function_status
                            and "error" not in function_status
                        ):
                            function_status = "FAILED"
                        rule_status[rl] = function_status
                else:
                    rule_status[rl] = "Rule number not implemented or present"

        except Exception as e:
            logger.error(f"Error in broadcast_rule_checker: {str(e)}")
            await self.broadcast(f"Error during rule checking: {str(e)}")
            return {"error": f"Error during rule checking: {str(e)}"}

        if not all_outputs:
            output_json_data = []  # empty list
        else:
            final_df = pd.concat(all_outputs, ignore_index=True)
            output_json_data = final_df[columns].to_dict(orient="records")

        # Save results locally if output file exists
        if output_file_path.exists():
            await self.broadcast(f"Results saved to local file: {output_file_path}")
        else:
            await self.broadcast(
                "No results to save - all rules returned empty results"
            )

        # return rule_status
        return output_json_data

    async def broadcast_connect(self):

        await self.broadcast("Socket Connected")


manager = ConnectionManager()


#################### API Routes ##################################


@app.get("/check")
async def get_task_name():
    return "Hello from server"


@app.post("/get_task_name")
async def get_task_name(payload: data_model, request: Request):
    try:
        input_dir = payload.input_dir_path
        print(f"Printing Payload {input_dir}")

        if not manager.active_connections:

            return {"status": "No clients connected"}

        # status_saved_path_folder = await manager.get_all_task_name(input_dir)

        return {"task_name": ["Task1", "Task2", "Task3"]}

    except Exception as e:
        return {"status": "Data Modelling Failed"}


@app.post("/data_modelling_api")
async def data_modelling_route(payload: data_model, request: Request):

    try:
        input_dir = payload.input_dir_path
        folder_name = payload.folder_name

        # Validate input parameters
        if not input_dir and not folder_name:
            return {"status": "Either input_dir_path or folder_name must be provided"}

        if input_dir and folder_name:
            return {"status": "Provide either input_dir_path or folder_name, not both"}

        # Use folder_name or input_dir directly - broadcast_data_modelling handles folder existence check
        target_input = folder_name if folder_name else input_dir

        print(f"Processing target: {target_input}")

        if not manager.active_connections:
            return {"status": "No clients connected"}

        # Run data modeling in background to prevent blocking WebSocket connections
        asyncio.create_task(manager.broadcast_data_modelling(target_input))

        return {
            "status": "Data modeling started. Check WebSocket for progress updates."
        }

    except Exception as e:
        logger.error(f"Data Modelling Failed: {e}")
        return {"status": "Data Modelling Failed"}


@app.post("/rule_checker_api")
async def rule_checker_route(payload: rule_check_model, request: Request):

    try:
        folder_name = payload.folder_name
        in_list = payload.input_list

        # Parse folder structure
        model_files_dir = Path("model_files")
        main_folder_path = model_files_dir / folder_name

        if not main_folder_path.exists():
            return {"error": f"Folder {folder_name} not found in model_files directory"}

        # Find the subfolder (should be the only directory inside main folder)
        subfolders = [item for item in main_folder_path.iterdir() if item.is_dir()]
        if not subfolders:
            return {"error": f"No subfolder found in {folder_name}"}

        # Use the first subfolder as data_model_input_path
        data_model_input_path = str(subfolders[0])

        # Find CSV files starting with "Task-" to get the full file path
        input_image = None
        for file in main_folder_path.iterdir():
            if (
                file.is_file()
                and file.name.endswith(".csv")
                and file.name.startswith("Task-")
            ):
                # Use the full file path instead of just the extracted name
                input_image = str(file)
                break

        # If Task-*.csv file is not found, continue without it instead of returning an error
        # This allows the coding checker process to continue even if the Task-csv file doesn't exist

        print("*" * 100)
        print(
            "folder_name",
            folder_name,
            "data_model_input_path",
            data_model_input_path,
            "input_image",
            input_image,
            "in_list",
            in_list,
        )

        if not manager.active_connections:
            return {"status": "No clients connected"}

        # Pass input_image to broadcast_rule_checker even if it's None
        # The rule checker functions will need to handle the case where input_image is None
        output_json_data = await manager.broadcast_rule_checker(
            data_model_input_path, in_list, input_image
        )
        return output_json_data

    except Exception as e:
        logger.error(f"Error in rule_checker_api: {str(e)}")
        return {"error": f"Rule Checker failed: {str(e)}"}


###########Throw the data to Web socket ##########################33


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        await manager.broadcast_connect()

        # Start ping task to keep connection alive with longer intervals for stability
        async def ping_task():
            while True:
                try:
                    await asyncio.sleep(20)  # Send ping every 20 seconds
                    await websocket.ping()
                    # Wait for pong with timeout
                    await asyncio.wait_for(websocket.pong(), timeout=10)
                except asyncio.TimeoutError:
                    logger.warning(
                        "WebSocket ping timeout - connection may be unstable"
                    )
                    break
                except Exception as e:
                    logger.error(f"WebSocket ping error: {e}")
                    break

        ping_coroutine = asyncio.create_task(ping_task())

        try:
            while True:
                # Use timeout for receive to prevent hanging
                try:
                    data = await asyncio.wait_for(websocket.receive_text(), timeout=60)
                    await manager.broadcast(f"Client says: {data}")
                except asyncio.TimeoutError:
                    # Send keepalive message if no data received
                    await websocket.send_text(
                        '{"type": "keepalive", "message": "Connection active"}'
                    )
        finally:
            ping_coroutine.cancel()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        await manager.broadcast("A client has disconnected.")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)
        await manager.broadcast("A client has disconnected due to error.")
