# LargeScale-Project
## Project Description
This project takes in input data and processes it to:

- **Resolve URL redirects** — ensuring that URLs point to the final destination pages.
- **Identify mutual links** — discovering links that are shared between data entries.
- The processed output is stored in Parquet format for efficient storage and transfer, especially in cloud environments.
  
## Requirements
- Python 3.12.4
- AWS credentials for S3 access
- Dependencies listed in Dependencies.txt .
- To install all dependencies listed in a Dependencies.txt file, you can use the following pip command:
>pip install -r requirements.txt

## Entrypoint Details
The entrypoint file for this project is entrypoint.py, which imports the main functionality from LSA_1_Project.py.

## How to Run the Project

1. Clone the repository.
2. Set the `PAGE_PAIRS_OUTPUT` environment variable to your desired S3 URI.
3. Run the project using `entrypoint.py`.

# Generating the Zip File
To package the project files for deployment, use the following PowerShell command:
```powershell
Compress-Archive -Path LSA_1_Project.py -DestinationPath LSA_1_Project.zip"
