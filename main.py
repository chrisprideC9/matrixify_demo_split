import streamlit as st
import pandas as pd
import zipfile
import io
import os

def split_csv(df, chunk_size=10):
    """
    Splits a DataFrame into multiple DataFrames each with up to chunk_size rows.

    Args:
        df (pd.DataFrame): The original DataFrame.
        chunk_size (int): Number of data rows per split CSV.

    Returns:
        List[pd.DataFrame]: A list of split DataFrames.
    """
    # Calculate the number of chunks needed
    num_chunks = (len(df) - 1) // chunk_size + 1  # -1 because header is not counted

    return [df.iloc[i*chunk_size:(i+1)*chunk_size] for i in range(num_chunks)]

def convert_df_to_csv(df):
    """
    Converts a DataFrame to CSV in memory.

    Args:
        df (pd.DataFrame): The DataFrame to convert.

    Returns:
        bytes: CSV data in bytes.
    """
    return df.to_csv(index=False).encode('utf-8')

def main():
    st.title("CSV Splitter Application")
    st.write("""
        Upload a CSV file, and this app will split it into multiple CSV files, each containing up to 10 data rows plus the header.
        The split files will be zipped into a single archive, with all CSVs inside a folder named `split_files`, for easy download.
    """)

    uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

    if uploaded_file is not None:
        try:
            # Read the CSV file into a pandas DataFrame
            df = pd.read_csv(uploaded_file)
            st.success("File uploaded successfully!")

            # Display the DataFrame
            st.write("### Original CSV:")
            st.dataframe(df)

            # Split the DataFrame into chunks
            split_dfs = split_csv(df, chunk_size=10)
            st.write(f"### The CSV will be split into {len(split_dfs)} file(s).")

            # Create a Zip archive in memory
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                # Define the folder name inside the zip
                folder_name = "split_files"

                for idx, split_df in enumerate(split_dfs, start=1):
                    # Get the original filename without extension
                    original_filename = os.path.splitext(uploaded_file.name)[0]
                    # Create new filename with incrementing number
                    new_filename = f"{original_filename}{idx}.csv"
                    # Path inside the zip file
                    zip_path = os.path.join(folder_name, new_filename)
                    # Convert split DataFrame to CSV bytes
                    csv_bytes = convert_df_to_csv(split_df)
                    # Add the CSV file to the zip archive inside the designated folder
                    zip_file.writestr(zip_path, csv_bytes)

            # Seek to the beginning of the BytesIO buffer
            zip_buffer.seek(0)

            # Provide a download button for the zip file
            st.download_button(
                label="Download Split CSVs",
                data=zip_buffer,
                file_name="split_csvs.zip",
                mime="application/zip"
            )

        except Exception as e:
            st.error(f"An error occurred while processing the file: {e}")

if __name__ == "__main__":
    main()
