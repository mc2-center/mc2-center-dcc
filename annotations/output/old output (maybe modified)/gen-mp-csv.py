import os
import csv

def get_publication_xlsx_files(folder_path):
    publication_xlsx_files = []
    for file in os.listdir(folder_path):
        if file.endswith("publication.xlsx"):
            publication_xlsx_files.append(os.path.join(folder_path, file))
    return publication_xlsx_files

def write_file_paths_to_csv(file_paths, output_file):
    with open(output_file, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["File Paths"])
        csv_writer.writerows([[file_path] for file_path in file_paths])

def main():
    folder_path = "/Users/agopalan/output"
    output_csv_file = "output_file_paths.csv"

    publication_xlsx_files = get_publication_xlsx_files(folder_path)
    write_file_paths_to_csv(publication_xlsx_files, output_csv_file)

    print("CSV file with publication file paths generated.")

if __name__ == "__main__":
    main()
