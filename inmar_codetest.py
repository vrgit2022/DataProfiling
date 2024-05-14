import pandas as pd
import os
import re

def CsvFilesCheckModule(source_directory_path, sink_directory_path, error_directory_path):
    ignored_files = []

    def clean_text(text):
        # Remove non-word characters and leading/trailing spaces
        return re.sub(r'\W+', ' ', str(text)).strip()

    def clean_phone_number(phone_number):
        # Remove any non-digit characters from the phone number
        return re.sub(r'\D', '', str(phone_number))

    try:
        dfs = []
        for file_name in os.listdir(source_directory_path):
            file_path = os.path.join(source_directory_path, file_name)
            if file_name.endswith('.csv') and os.path.isfile(file_path):
                if os.path.getsize(file_path) > 10240:
                    df = pd.read_csv(file_path)

                    # Remove spaces from the phone column
                    df['phone'] = df['phone'].str.replace(r'\s+', '')

                    # Clean and split descriptive fields
                    df['cleaned_address'] = df['address'].apply(clean_text)

                    # Extract contact numbers from the phone column
                    df[['contact_number1', 'contact_number2']] = df['phone'].str.split(r'\r\n', expand=True)

                    # Clean phone numbers
                    df['contact_number1'] = df['contact_number1'].apply(clean_phone_number)
                    df['contact_number2'] = df['contact_number2'].apply(clean_phone_number)

                    # Drop the 'phone' column
                    df.drop(columns=['phone'], inplace=True)

                    dfs.append(df)
                    # Push processed file to sink_directory_path
                    processed_file_path = os.path.join(sink_directory_path, file_name)
                    df.to_csv(processed_file_path, index=False)
                else:
                    ignored_files.append(file_name)

        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            return combined_df, ignored_files
        else:
            return None, ignored_files

    except Exception as e:
        print("An error occurred in CsvFilesCheckModule:", e)
        write_to_error_log("Error in CsvFilesCheckModule: " + str(e), error_directory_path)
        return None, None


def write_to_error_log(error_message, error_directory_path):
    try:
        if not os.path.exists(error_directory_path):
            os.makedirs(error_directory_path)

        error_log_file_path = os.path.join(error_directory_path, "error_log.txt")
        with open(error_log_file_path, "a") as error_log_file:
            error_log_file.write(error_message + "\n")

    except Exception as e:
        print("An error occurred while writing to error log:", e)


# Path
source_directory_path = "C:\\Users\\bahulayan\\pythontask\\DataProfiling\\Source\\"
sink_directory_path = "C:\\Users\\bahulayan\\pythontask\\DataProfiling\\Processed\\"
error_directory_path = "C:\\Users\\bahulayan\\pythontask\\DataProfiling\\Error\\"
combined_df, ignored_files = CsvFilesCheckModule(source_directory_path, sink_directory_path, error_directory_path)

if combined_df is not None:
    print("Ignored Files:")
    print(ignored_files)

    print("Transformed DataFrame:")
    print(combined_df)

    print("Split Phone Columns:")
    print(combined_df[['address', 'contact_number1', 'contact_number2']])
