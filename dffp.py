import sys

try:
    import os
    import logging
    import pandas as pd
    import chardet
    import magic
    from typing import List, Optional, Union, Dict

    print("All modules imported successfully!")
except ImportError as e:
    print(f"ImportError: {e}")
    sys.exit(1)


class DataFrameFileProcessor:
	"""
	A class to process files within directories using a pandas DataFrame.
	"""

	def __init__(self, parent_directory: str):
		"""
		Initializes the DataFrameFileProcessor with the given parent directory.

		Parameters:
			parent_directory (str): The root directory to process files from.

		Raises:
			FileNotFoundError: If the provided path does not exist.
			NotADirectoryError: If the provided path is not a directory.
		"""

		logging.basicConfig(
			level=logging.DEBUG,  # Changed to DEBUG for more detailed logs
			format='%(asctime)s [%(levelname)s] %(message)s',
			handlers=[
				logging.FileHandler("dataframe_file_processor.log"),
        logging.StreamHandler()
				])

		self.parent_directory = parent_directory
		self._validate_directory()
		self.files_df = pd.DataFrame(
			columns=['Filename', 'Path', 'FileExtension', 'Encoding', 'Magic']
		)
		self.filter_column = 'FilterResult'  # Column to store boolean filter results

	def _validate_directory(self):
		if not os.path.exists(self.parent_directory):
			logging.error(f"Path does not exist: {self.parent_directory}")
			raise FileNotFoundError(f"Path does not exist: {self.parent_directory}")
		if not os.path.isdir(self.parent_directory):
			logging.error(f"Path is not a directory: {self.parent_directory}")
			raise NotADirectoryError(f"Path is not a directory: {self.parent_directory}")
		logging.info(f"Initialized DataFrameFileProcessor for directory: {self.parent_directory}")

	def populate_dataframe(self):
		"""
		Populates the DataFrame by recursively scanning the parent folder.
		"""
		records = []
		for root, dirs, files in os.walk(self.parent_directory):
			for file in files:
				file_path = os.path.join(root, file)
				filename = os.path.basename(file_path)
				file_extension = os.path.splitext(filename)[1]
				records.append({
					'Filename': filename,
					'Path': file_path,
					'FileExtension': file_extension,
					'Encoding': None,  # To be filled later
					'Magic': None      # To be filled later
				})
		self.files_df = pd.DataFrame(records)
		logging.info(f"Populated DataFrame with {len(self.files_df)} files.")

	def analyze_files(self):
		"""
		Determines the Encoding and Magic type for each file in the DataFrame.
		"""
		encodings = []
		magics = []
		for idx, row in self.files_df.iterrows():
			file_path = row['Path']

			# Determine Encoding
			encoding = self._detect_encoding(file_path)
			encodings.append(encoding)

			# Determine Magic Type
			magic_type = self._detect_magic(file_path)
			magics.append(magic_type)

		self.files_df['Encoding'] = encodings
		self.files_df['Magic'] = magics
		logging.info("Analyzed files for Encoding and Magic type.")

	def _detect_encoding(self, file_path: str) -> Optional[str]:
		"""
		Detects the encoding of a file using chardet.

		Parameters:
			file_path (str): The path to the file.

		Returns:
			Optional[str]: The detected encoding or None if detection fails.
		"""
		try:
			with open(file_path, 'rb') as f:
				raw_data = f.read(4096)
			result = chardet.detect(raw_data)
			encoding = result['encoding']
			return encoding
		except Exception as e:
			logging.warning(f"Failed to detect encoding for {file_path}: {e}")
			return None

	def _detect_magic(self, file_path: str) -> Optional[str]:
		"""
		Detects the magic type of a file using python-magic.

		Parameters:
			file_path (str): The path to the file.

		Returns:
			Optional[str]: The magic type or None if detection fails.
		"""
		try:
			magic_type = magic.from_file(file_path, mime=True)
			return magic_type
		except Exception as e:
			logging.warning(f"Failed to detect magic type for {file_path}: {e}")
			return None

	def apply_filters(self, conditions: Union[Dict[str, List[str]], List[str], str]):
		"""
		Applies filters to the DataFrame based on user-specified conditions.

		Parameters:
			conditions (Union[Dict[str, List[str]], List[str], str]): The filtering conditions.

		Notes:
			- The conditions can be a dictionary with keys 'extensions', 'encodings', 'magic'.
			- If a single list or string is provided, the method will infer the condition type.
		"""
		# Initialize the filter column to True
		self.files_df[self.filter_column] = True

		# Determine the type of conditions provided
		if isinstance(conditions, dict):
			# Apply filters for each condition type
			if 'extensions' in conditions:
				self._filter_by_extension(conditions['extensions'])
			if 'encodings' in conditions:
				self._filter_by_encoding(conditions['encodings'])
			if 'magic' in conditions:
				self._filter_by_magic(conditions['magic'])
		elif isinstance(conditions, list):
			# Infer condition type
			self._infer_and_apply_single_condition(conditions)
		elif isinstance(conditions, str):
			# Infer condition type
			self._infer_and_apply_single_condition([conditions])
		else:
			logging.warning("Invalid conditions provided. No filters applied.")

	def _infer_and_apply_single_condition(self, condition_list: List[str]):
		"""
		Infers the condition type based on the data and applies the filter.

		Parameters:
			condition_list (List[str]): The list of conditions.
		"""
		# Convert condition_list elements to lowercase for case-insensitive comparison
		condition_set = set([item.lower() for item in condition_list])

		# Try to infer which column the conditions apply to
		if condition_set.intersection(set(self.files_df['FileExtension'].str.lower().unique())):
			self._filter_by_extension(condition_list)
		elif condition_set.intersection(set(self.files_df['Encoding'].dropna().str.lower().unique())):
			self._filter_by_encoding(condition_list)
		elif condition_set.intersection(set(self.files_df['Magic'].dropna().str.lower().unique())):
			self._filter_by_magic(condition_list)
		else:
			logging.warning("Could not infer condition type. No filters applied.")

	def _filter_by_extension(self, extensions: List[str]):
		"""
		Filters the DataFrame by file extensions.

		Parameters:
			extensions (List[str]): List of acceptable file extensions.
		"""
		extensions = [ext.lower() for ext in extensions]
		self.files_df[self.filter_column] &= self.files_df['FileExtension'].str.lower().isin(extensions)
		logging.info(f"Applied extension filter: {extensions}")

	def _filter_by_encoding(self, encodings: List[str]):
		"""
		Filters the DataFrame by file encodings.

		Parameters:
			encodings (List[str]): List of acceptable encodings.
		"""
		encodings = [enc.lower() for enc in encodings]
		self.files_df[self.filter_column] &= self.files_df['Encoding'].str.lower().isin(encodings)
		logging.info(f"Applied encoding filter: {encodings}")

	def _filter_by_magic(self, magic_types: List[str]):
		"""
		Filters the DataFrame by magic types.

		Parameters:
			magic_types (List[str]): List of acceptable magic types.
		"""
		magic_types = [m.lower() for m in magic_types]
		self.files_df[self.filter_column] &= self.files_df['Magic'].str.lower().isin(magic_types)
		logging.info(f"Applied magic type filter: {magic_types}")

	def get_filtered_files(self) -> List[str]:
		"""
		Returns a list of file paths that pass the filtering conditions.

		Returns:
			List[str]: List of file paths.
		"""
		filtered_df = self.files_df[self.files_df[self.filter_column]]
		logging.info(f"Number of files after filtering: {len(filtered_df)}")
		return filtered_df['Path'].tolist()
