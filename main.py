from dffp import DataFrameFileProcessor

def main():
    # Initialize the processor with the parent directory
    parent_directory = "/home/tejas_adg/Downloads/Cpp_Programs"
    processor = DataFrameFileProcessor(parent_directory)

    # Populate the DataFrame with file information
    processor.populate_dataframe()

    # Analyze files to determine encodings and magic types
    processor.analyze_files()

    # Define filtering conditions
    conditions = {
        'extensions': ['.cpp', '.h', '.hpp'],
        'encodings': ['utf-8'],
        'magic': ['text/plain']
    }

    # Apply filters based on conditions
    processor.apply_filters(conditions)

    # Get the list of files that pass the filters
    filtered_files = processor.get_filtered_files()

    # Print the filtered files
    print("Filtered Files:")
    for file in filtered_files:
        print(file)

if __name__ == "__main__":
    main()
