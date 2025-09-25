# VoltaV
Flask app for filtering/visualizing parquet data.

## Quickstart
For development, clone the repository and once you are in the NEDCO_data_viz_app directory, create a .env file with these two variables and fill them in
```sh
BUCKET_URL = ""
SUPABASE_KEY=""
```

To run this app locally, run this in the command line (you might need to replace "python" with "python3")
```sh
python run.py
```

## Generating A One Click File for Windows/Mac

To generate a .exec for MacOS and Windows, you can just push to the main branch and it will be generated.

If manually generating, go to GitHub -> Actions -> Package Into One Click (on the left under "All workflows") -> Run workflow

After completion (both automatic and manual), go to GitHub -> Actions -> Click the workflow run -> Scroll to "Artifacts" at the bottom and you can see two files: volta-macos and volta-windows. This should download a zip file to your computer which, after extraction, will contain the one-click package

Note: If you are sending these files to other people, first compress into a zip file and then send
