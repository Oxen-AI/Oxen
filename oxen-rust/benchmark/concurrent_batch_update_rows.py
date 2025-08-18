from pprint import pprint
import random
from typing import Any
from oxen import RemoteRepo
import pandas as pd
from concurrent.futures import Future, ThreadPoolExecutor
import requests
from dataclasses import dataclass
import lorem
from tqdm import tqdm

N_WORKERS = 16
num_entries = 100_000

BASE_URL = "http://localhost:3000"
TEST_NAMESPACE = "ox"
TEST_REPO = "testing"
ITERATION = 14
TEST_FILE = f"data{ITERATION}.parquet"
TEST_WORKSPACE_NAME = f"test_workspace_{ITERATION}"
TEST_WORKSPACE_ID = f"00dcbbda-606b-4883-b35b-ccf595ab555{ITERATION}"


@dataclass
class Workspace:
    id: str
    name: str

def parseWorkspace(data: dict[str, Any]) -> Workspace:
    """Convert a parsed json dict into Workspace dataclass"""
    pprint(data)
    return Workspace(id=data["id"], name=data["name"])

@dataclass
class DataRow:
    _oxen_id: str

def parseDataRow(data: dict[str, Any]) -> DataRow:
    """Convert a parsed json dict into DataRow dataclass"""
    return DataRow(_oxen_id=data["_oxen_id"])

@dataclass
class DataFrameSize:
    height: int
    width: int

def parseDataFrameSize(data: dict[str, Any]) -> DataFrameSize:
    """Convert a parsed json dict into DataFrameSize dataclass"""
    height = data.get("height", 0)
    width = data.get("width", 0)
    return DataFrameSize(height=height, width=width)

@dataclass
class DataFrameView:
    size: DataFrameSize
    data: list[DataRow]

def parseDataFrameView(data: dict[str, Any]) -> DataFrameView:
    """Convert a parsed json dict into DataFrameView dataclass"""
    if "data" in data:
        data["data"] = [parseDataRow(row) for row in data["data"]]
        size = parseDataFrameSize(data.get("size", {}))
        return DataFrameView(size=size, data=data.get("data", []))
    else:
        raise ValueError("Expected 'data' key in DataFrameView response, got None or missing key")

@dataclass
class DataFrame:
    view: DataFrameView
    size: DataFrameSize

def parseDataFrame(data: dict[str, Any]) -> DataFrame:
    """Convert a parsed json dict into DataFrame dataclass"""
    if "view" in data:
        data["view"] = parseDataFrameView(data["view"])
        size = parseDataFrameSize(data.get("size", {}))
        return DataFrame(view=data.get("view", []), size=size)
    else:
        raise ValueError("Expected 'view' key in DataFrame response, got None or missing key")


@dataclass
class OxenResponse:
    oxen_version: str
    status: str
    status_message: str
    workspaces: list[Workspace] | None = None
    workspace: Workspace | None = None
    data_frame: DataFrame | None = None

def parseOxenResponse(data: dict[str, Any]) -> OxenResponse:
    """Convert a parsed json dict into OxenResponse dataclass"""
    if "workspaces" in data:
        data["workspaces"] = [parseWorkspace(wsDict) for wsDict in data.get("workspaces", [])]
    elif "workspace" in data:
        data["workspace"] = parseWorkspace(data["workspace"])
    elif "data_frame" in data:
        data["data_frame"] = parseDataFrame(data["data_frame"])
    return OxenResponse(
        oxen_version=data.get("oxen_version", ""),
        status=data.get("status", ""),
        status_message=data.get("status_message", ""),
        workspaces=data.get("workspaces", None),
        workspace=data.get("workspace", None),
        data_frame=data.get("data_frame", None)
    )


def get_workspaces() -> list[Workspace]:
    """Fetch workspaces from the Oxen API"""
    response = requests.get(f"{BASE_URL}/api/repos/{TEST_NAMESPACE}/{TEST_REPO}/workspaces")
    response.raise_for_status()
    data = parseOxenResponse(response.json())
    print(f"get_workspaces() data: {data}")
    if data.workspace:
        return [data.workspace]
    elif data.workspaces != None:
        return data.workspaces
    else:
        raise Exception("Expected workspaces in response, got None")

def create_workspace() -> Workspace:
    """Create a new workspace in the repository"""
    url = f"{BASE_URL}/api/repos/{TEST_NAMESPACE}/{TEST_REPO}/workspaces"
    response = requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json={"name": TEST_WORKSPACE_NAME, "branch_name": "main", "workspace_id": TEST_WORKSPACE_ID},
    )
    response.raise_for_status()
    data = parseOxenResponse(response.json())
    if not data.workspace:
        raise Exception("Excpected workspace in response, got None")
    return data.workspace


def generate_initial_data():
    """Generate initial data with prompts and responses"""
    print(f"Generating {num_entries:,} initial entries...")
    
    data = []
    for _ in tqdm(range(num_entries), desc="Generating data", unit="entries"):
        # Generate random prompt
        prompt = lorem.sentence()
        
        # Generate random response
        response = lorem.paragraph()
        
        data.append({
            "prompt": prompt,
            "response": response,
            "new_response": ""  # Empty initially
        })
    
    return pd.DataFrame(data)


def update_row(workspace_id: str, row_id : str) -> str:
    """Process a single row from the DataFrame"""
    url = f"{BASE_URL}/api/repos/{TEST_NAMESPACE}/{TEST_REPO}/workspaces/{workspace_id}/data_frames/rows/resource/{TEST_FILE}"
    test_data = {
        "data": [{
            "row_id": row_id,
            "value": {
                "new_response": f"random {random.randint(1, 1000)} {lorem.sentence()}"
            }
        }]
    }
    response = requests.put(
        url, 
        json=test_data,
        headers={"Content-Type": "application/json"}
    )
    response.raise_for_status()
    return row_id


def get_row(workspace_id: str, row_id: str):
    """Get a single row """
    try:
        url = f"{BASE_URL}/api/repos/{TEST_NAMESPACE}/{TEST_REPO}/workspaces/{workspace_id}/data_frames/rows/{row_id}/resource/{TEST_FILE}"
        response = requests.get(url)
        response.raise_for_status()
        return row_id, response.json()
    except Exception as e:
        print(f"Error processing row {row_id}: {e}")
        return row_id, None


def fetch_all_rows(workspace_id: str, file_path: str, page_size: int = 1000, total_rows: int | None = None) -> list[DataRow]:
    """Fetch all rows from the DataFrame using pagination"""
    all_rows = []
    page = 0
    
    # If we don't have total_rows, we can't show a progress bar
    if total_rows is None:
        print("Fetching rows without size information...")
        while True:
            url = f"{BASE_URL}/api/repos/{TEST_NAMESPACE}/{TEST_REPO}/workspaces/{workspace_id}/data_frames/resource/{file_path}"
            params = {"page": page, "page_size": page_size}
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            parsed_response = parseOxenResponse(response.json())
            if not parsed_response.data_frame:
                raise Exception("Expected data_frame in response, got None")
            
            df = parsed_response.data_frame
            current_page_rows = df.view.data
            
            if not current_page_rows:
                break
                
            all_rows.extend(current_page_rows)
            print(f"Fetched page {page + 1}: {len(current_page_rows)} rows (total so far: {len(all_rows)})")
            
            # If we got fewer rows than page_size, we've reached the end
            if len(current_page_rows) < page_size:
                break
                
            page += 1
    else:
        # We have total_rows, so we can show a progress bar
        with tqdm(total=total_rows, desc="Fetching rows", unit="rows") as pbar:
            while True:
                url = f"{BASE_URL}/api/repos/{TEST_NAMESPACE}/{TEST_REPO}/workspaces/{workspace_id}/data_frames/resource/{file_path}"
                params = {"page": page, "page_size": page_size}
                
                response = requests.get(url, params=params)
                response.raise_for_status()
                
                parsed_response = parseOxenResponse(response.json())
                if not parsed_response.data_frame:
                    raise Exception("Expected data_frame in response, got None")
                
                df = parsed_response.data_frame
                current_page_rows = df.view.data
                
                if not current_page_rows:
                    break
                    
                all_rows.extend(current_page_rows)
                pbar.update(len(current_page_rows))
                
                # If we got fewer rows than page_size, we've reached the end
                if len(current_page_rows) < page_size:
                    break
                    
                page += 1
    
    return all_rows


def main():
    repo = RemoteRepo(f"{TEST_NAMESPACE}/{TEST_REPO}", host="localhost:3000", scheme="http")

    if not repo.exists():
        print("Creating repo")
        repo.create()

    file_name = TEST_FILE
    
    # Create initial parquet file with 100,000 rows if it doesn't exist
    if not repo.file_exists(file_name):
        print(f"Creating initial {TEST_FILE} with 100,000 rows...")
        df = generate_initial_data()
        df.to_parquet(file_name)
        repo.add(file_name)
        repo.commit(f"Add initial {TEST_FILE} with 100,000 rows")
        print("Initial parquet file created and committed successfully!")
    else:
        print(f"{TEST_FILE} already exists, proceeding with updates...")

    # Get existing workspace or create a new one
    # existing_workspaces = get_workspaces()
    # if not existing_workspaces:
    print("No existing workspaces found, creating a new one...")
    workspace = create_workspace()
    assert workspace.id == TEST_WORKSPACE_ID, f"Expected workspace ID {TEST_WORKSPACE_ID}, got {workspace.id}"
    # else:
    #     print(f"Using existing workspace: {existing_workspaces[0].name}")
    #     workspace = existing_workspaces[0]
    
    # Index the DataFrame
    response = requests.put(
        f"{BASE_URL}/api/repos/{TEST_NAMESPACE}/{TEST_REPO}/workspaces/{workspace.id}/data_frames/resource/{TEST_FILE}",
        headers={"Content-Type": "application/json"},
        json={"is_indexed": True}
    )
    response.raise_for_status()
    
    # Get the total size first to know how many rows we're dealing with
    # We'll use a simple approach - fetch the first page to get size info
    print("Getting DataFrame size information...")
    size_response = requests.get(f"{BASE_URL}/api/repos/{TEST_NAMESPACE}/{TEST_REPO}/workspaces/{workspace.id}/data_frames/resource/{TEST_FILE}?page=0&page_size=1")
    size_response.raise_for_status()
    
    try:
        size_data = parseOxenResponse(size_response.json())
        if not size_data.data_frame:
            raise Exception("Expected data_frame in response, got None")
        
        total_rows = size_data.data_frame.size.height
        print(f"DataFrame size: {total_rows} rows x {size_data.data_frame.size.width} columns")
    except Exception as e:
        print(f"Warning: Could not parse size information: {e}")
        print("Falling back to fetching all rows without size info...")
        total_rows = None

    # Fetch all rows using pagination
    if total_rows:
        print(f"Fetching all {total_rows:,} rows using pagination...")
    else:
        print("Fetching all rows using pagination (size unknown)...")
    
    all_rows = fetch_all_rows(workspace.id, TEST_FILE, total_rows=total_rows)
    print(f"Fetched {len(all_rows):,} rows total")

    # Use ThreadPoolExecutor to process rows in parallel
    with ThreadPoolExecutor(max_workers=N_WORKERS) as executor:
        # Submit tasks for each row
        futures : list[Future[str]] = []
        total_rows_to_process = len(all_rows)
        print(f"Submitting {total_rows_to_process:,} rows for processing...")
        
        for row in tqdm(all_rows, desc="Submitting tasks", total=total_rows_to_process, unit="rows"):
            future : Future[str] = executor.submit(update_row, workspace.id, row._oxen_id)
            futures.append(future)
        
        # Collect results
        print("collecting results...")
        results = []
        has_error = False
        total_rows = len(futures)
        completed_rows = 0
        
        for future in tqdm(futures, desc="Collecting results", total=total_rows):
            try:
                row_index = future.result(timeout=30)  # 30 second timeout per row
                results.append(row_index)
                completed_rows += 1
            except Exception as e:
                has_error = True
                print(f"Future failed: {e}")
        
        print(f"Successfully processed {completed_rows}/{total_rows} rows")
        
    if has_error:
        print("Some futures failed, check logs for details.")
    else:
        print("All futures completed successfully.")


if __name__ == "__main__":
    main()