import requests
import pandas as pd
import sqlite3
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

class API:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "authority": "obd.hcraontario.ca",
                "accept": "application/json, text/plain, */*",
                "accept-language": "es-ES,es;q=0.8",
                "referer": "https://obd.hcraontario.ca",
                "sec-ch-ua": '"Not A(Brand";v="99", "Brave";v="121", "Chromium";v="121"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "sec-gpc": "1",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            }
        )

    def search_builder(
        self,
        builderName: str = None,
        builderLocation: str = None,
        builderNum: str = None,
        officerDirector: str = None,
        umbrellaCo: str = None,
        licenceStatus: str = None,
        yearsActive: str = None,
    ) -> list:
        """
        Performs a search for builders based on various criteria.

        Parameters:
        - builderName (str): Name of the builder (optional).
        - builderLocation (str): Location of the builder (optional).
        - builderNum (str): Number of the builder (optional).
        - officerDirector (str): Name of the officer or director (optional).
        - umbrellaCo (str): Umbrella company (optional).
        - licenceStatus (str): License status (optional).
        - yearsActive (str): Years of activity (optional).

        Returns:
        - A list of dictionaries, where each dictionary represents a match with the search criteria.
        """
        params = {
            "builderName": builderName,
            "builderLocation": builderLocation,
            "builderNum": builderNum,
            "officerDirector": officerDirector,
            "umbrellaCo": umbrellaCo,
            "licenceStatus": licenceStatus,
            "yearsActive": yearsActive,
            "page": "1",
        }

        response = self.session.get(
            "https://obd.hcraontario.ca/api/builders", params=params
        )

        return response.json()

    def get_builder_detail(self, ID: str) -> dict:
        """
        Retrieves comprehensive details for a specific builder using its ID.

        Parameters:
        - ID (str): The unique identifier for the builder.

        Returns:
        - A dictionary with all relevant information about the builder, including:
            * 'summary': Summary of the builder.
            * 'PDOs': Development projects.
            * 'convictions': Legal convictions.
            * 'conditions': Conditions.
            * 'members': Members.
            * 'properties': Properties.
            * 'enrolments': Enrolments.
            * 'condoProjects': Condo projects.

        The function utilizes ThreadPoolExecutor for concurrent HTTP GET requests to various endpoints,
        improving efficiency and reducing overall execution time by parallelizing network I/O operations.
        """
        self.ID = ID  # Makes ID available throughout the instance
        urls = {
            "summary": "https://obd.hcraontario.ca/api/buildersummary",
            "PDOs": "https://obd.hcraontario.ca/api/builderPDOs",
            "convictions": "https://obd.hcraontario.ca/api/builderConvictions",
            "conditions": "https://obd.hcraontario.ca/api/builderConditions",
            "members": "https://obd.hcraontario.ca/api/builderMembers",
            "properties": "https://obd.hcraontario.ca/api/builderProperties",
            "enrolments": "https://obd.hcraontario.ca/api/builderEnrolments",
            "condoProjects": "https://obd.hcraontario.ca/api/builderCondoProjects",
        }

        data = {}
        with ThreadPoolExecutor() as executor:
            # Submits all fetch_url tasks to the ThreadPoolExecutor for concurrent execution.
            futures = {
                executor.submit(self.__fetch_url, item): item for item in urls.items()
            }
            for future in as_completed(futures):
                # As each future completes, extracts and stores the result using the key in the data dictionary.
                key, result = future.result()
                data[key] = result

        return data
    #NEW ALL
    def get_umbrella_detail(self, ID: str) -> dict:
        """
        Retrieves comprehensive details for a specific umbrella company using its ID.

        Parameters:
        - ID (str): The unique identifier for the umbrella company.

        Returns:
        - A dictionary with all relevant information about the umbrella company, including:
            * 'summary': Summary of the umbrella company.
            * 'properties': Properties under the umbrella company.
            * 'members': Member builders of the umbrella company.
            * 'condoProjects': Condo projects under the umbrella company.
            * 'enrolments': Enrolments under the umbrella company.

        The function utilizes ThreadPoolExecutor for concurrent HTTP GET requests to various endpoints,
        improving efficiency and reducing overall execution time by parallelizing network I/O operations.
        """
        self.ID = ID  # Makes ID available throughout the instance
        urls = {
            "summary": "https://obd.hcraontario.ca/api/umbrellaSummary",
            "properties": "https://obd.hcraontario.ca/api/umbrellaProperties",
            "members": "https://obd.hcraontario.ca/api/umbrellaMembers",  # Corrected URL
            "condoProjects": "https://obd.hcraontario.ca/api/umbrellaCondoProjects",
            "enrolments": "https://obd.hcraontario.ca/api/umbrellaEnrolments",
        }

        data = {}
        with ThreadPoolExecutor() as executor:
            # Submits all fetch_url tasks to the ThreadPoolExecutor for concurrent execution.
            futures = {
                executor.submit(self.__fetch_url, item): item for item in urls.items()
            }
            for future in as_completed(futures):
                # As each future completes, extracts and stores the result using the key in the data dictionary.
                key, result = future.result()
                data[key] = result

        return data
    #END NEW
    #START NEW SAVE FUNCTIONALITY
    def save_to_csv(self, data, base_filename: str, ID: str, directory: str = "") -> None:
        """
        Saves API results to CSV files.

        Parameters:
        - data: The data to save (list or dict)
        - base_filename: Base name for the CSV file
        - ID: Identifier for the file naming
        - directory: Optional directory path for saving files
        """
        import os
        
        if isinstance(data, list):
            df = pd.DataFrame(data)
            filename = f"{base_filename}_{ID}.csv"
            if directory:
                filename = os.path.join(directory, filename)
            df.to_csv(filename, index=False)
            print(f"Saved search results to {filename}")
            
        elif isinstance(data, dict):
            for key, value in data.items():
                if value:  # Only save if there's data
                    df = pd.DataFrame(value)
                    filename = f"{base_filename}_{ID}_{key}.csv"
                    if directory:
                        filename = os.path.join(directory, filename)
                    df.to_csv(filename, index=False)
                    print(f"Saved {key} data to {filename}")

    def save_to_xlsx(self, data, filename: str, ID: str, directory: str = "") -> None:
        """
        Saves API results to a single Excel file with multiple sheets.

        Parameters:
        - data: The data to save (list or dict)
        - filename: Name for the Excel file
        - ID: Identifier for the file naming
        - directory: Optional directory path for saving files
        """
        filepath = f"{filename}_{ID}.xlsx"
        if directory:
            filepath = os.path.join(directory, filepath)

        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            if isinstance(data, list):
                df = pd.DataFrame(data)
                df.to_excel(writer, sheet_name='Search_Results', index=False)
                print(f"Saved search results to sheet 'Search_Results'")
            
            elif isinstance(data, dict):
                for key, value in data.items():
                    if value:
                        try:
                            if isinstance(value, list):
                                df = pd.DataFrame(value)
                            elif isinstance(value, dict):
                                df = pd.DataFrame([value])
                            else:
                                print(f"Skipping {key}: Unsupported data type")
                                continue
                            
                            if not df.empty:
                                sheet_name = key[:31]  # Excel sheet name length limit
                                df.to_excel(writer, sheet_name=sheet_name, index=False)
                                print(f"Saved {key} data to sheet '{sheet_name}'")
                        except Exception as e:
                            print(f"Error saving {key}: {str(e)}")

        print(f"Excel file saved as: {filepath}")

    def save_to_sql(self, data, db_name: str, ID: str, directory: str = "") -> None:
        """
        Saves API results to a SQLite database.

        Parameters:
        - data: The data to save (list or dict)
        - db_name: Name for the database file
        - ID: Identifier for the file naming
        - directory: Optional directory path for saving files
        """
        db_path = f"{db_name}_{ID}.db"
        if directory:
            db_path = os.path.join(directory, db_path)

        conn = sqlite3.connect(db_path)

        try:
            if isinstance(data, list):
                df = pd.DataFrame(data)
                table_name = 'search_results'
                df.to_sql(table_name, conn, if_exists='replace', index=False)
                print(f"Saved search results to table '{table_name}'")
            
            elif isinstance(data, dict):
                for key, value in data.items():
                    if value:
                        try:
                            if isinstance(value, list):
                                df = pd.DataFrame(value)
                            elif isinstance(value, dict):
                                df = pd.DataFrame([value])
                            else:
                                print(f"Skipping {key}: Unsupported data type")
                                continue
                            
                            if not df.empty:
                                table_name = ''.join(c if c.isalnum() else '_' for c in key)
                                df.to_sql(table_name, conn, if_exists='replace', index=False)
                                print(f"Saved {key} data to table '{table_name}'")
                        except Exception as e:
                            print(f"Error saving {key}: {str(e)}")

        finally:
            conn.close()

        print(f"SQLite database saved as: {db_path}")

    #END NEW SAVE FUNCTIONALITY

    #START NEW MASTER LIST SAVE
    def save_multiple_to_master_csv(self, ids: list, is_umbrella: bool = False, directory: str = "") -> None:
        """
        Saves data for multiple builders or umbrella companies to consolidated CSV files.
        Creates one CSV file per data type, containing data from all IDs.
        
        Parameters:
        - ids: List of builder or umbrella IDs to process
        - is_umbrella: Set to True for umbrella companies, False for builders
        - directory: Optional directory path for saving files
        """
        
        type_label = "umbrella" if is_umbrella else "builder"
        # Dictionary to store combined data for each data type
        combined_data = {}
        
        for idx, id in enumerate(ids):
            print(f"Processing {type_label} ID {id} ({idx+1}/{len(ids)})...")
            
            try:
                # Get the data depending on whether it's a builder or umbrella
                if is_umbrella:
                    data = self.get_umbrella_detail(id)
                else:
                    data = self.get_builder_detail(id)
                
                # Combine data by data type
                for key, value in data.items():
                    if value:
                        if key not in combined_data:
                            combined_data[key] = []
                        
                        # Ensure we're working with a list of records
                        if isinstance(value, list):
                            # Add ID to each record
                            for item in value:
                                if isinstance(item, dict):  # Make sure it's a dict before adding ID
                                    item['source_id'] = id
                            combined_data[key].extend(value)
                        elif isinstance(value, dict):
                            value['source_id'] = id
                            combined_data[key].append(value)
            
            except Exception as e:
                print(f"Error processing ID {id}: {str(e)}")
        
        # Save all combined data to CSV files
        for key, data_list in combined_data.items():
            if data_list:
                df = pd.DataFrame(data_list)
                filename = f"{type_label}_{key}_master.csv"
                if directory:
                    filename = os.path.join(directory, filename)
                df.to_csv(filename, index=False)
                print(f"Saved combined {key} data ({len(data_list)} records) to {filename}")
        
        print(f"Completed processing {len(ids)} {type_label} IDs into master CSV files.")
    
    def save_multiple_to_master_xlsx(self, ids: list, is_umbrella: bool = False, directory: str = "") -> None:
        """
        Saves data for multiple builders or umbrella companies to a single Excel file.
        Each data type becomes a separate sheet in the master Excel file.
        
        Parameters:
        - ids: List of builder or umbrella IDs to process
        - is_umbrella: Set to True for umbrella companies, False for builders
        - directory: Optional directory path for saving files
        """
        
        type_label = "umbrella" if is_umbrella else "builder"
        # Dictionary to store combined data for each data type
        combined_data = {}
        
        for idx, id in enumerate(ids):
            print(f"Processing {type_label} ID {id} ({idx+1}/{len(ids)})...")
            
            try:
                # Get the data depending on whether it's a builder or umbrella
                if is_umbrella:
                    data = self.get_umbrella_detail(id)
                else:
                    data = self.get_builder_detail(id)
                
                # Combine data by data type
                for key, value in data.items():
                    if value:
                        if key not in combined_data:
                            combined_data[key] = []
                        
                        # Ensure we're working with a list of records
                        if isinstance(value, list):
                            # Add ID to each record
                            for item in value:
                                if isinstance(item, dict):
                                    item['source_id'] = id
                            combined_data[key].extend(value)
                        elif isinstance(value, dict):
                            value['source_id'] = id
                            combined_data[key].append(value)
            
            except Exception as e:
                print(f"Error processing ID {id}: {str(e)}")
        
        # Create master Excel file with all data
        if combined_data:
            filename = f"{type_label}_master.xlsx"
            if directory:
                filename = os.path.join(directory, filename)
                
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                for key, data_list in combined_data.items():
                    if data_list:
                        df = pd.DataFrame(data_list)
                        # Excel has a 31 character limit for sheet names
                        sheet_name = key[:31]
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"Saved combined {key} data ({len(data_list)} records) to sheet '{sheet_name}'")
            
            print(f"Master Excel file saved as: {filename}")
        else:
            print("No data was collected to save to Excel.")
        
        print(f"Completed processing {len(ids)} {type_label} IDs into master Excel file.")
    
    def save_multiple_to_master_sql(self, ids: list, is_umbrella: bool = False, db_name: str = "master_database", directory: str = "") -> None:
        """
        Saves data for multiple builders or umbrella companies to a single SQLite database.
        Creates one table per data type, containing data from all IDs.
        
        Parameters:
        - ids: List of builder or umbrella IDs to process
        - is_umbrella: Set to True for umbrella companies, False for builders
        - db_name: Name for the master database file
        - directory: Optional directory path for saving the database
        """
        
        type_label = "umbrella" if is_umbrella else "builder"
        
        # Create database path
        db_path = f"{db_name}.db"
        if directory:
            db_path = os.path.join(directory, db_path)
        
        # Create connection to SQLite database
        conn = sqlite3.connect(db_path)
        
        try:
            # Create a master table to track all processed IDs
            master_df = pd.DataFrame({"id": ids, "processed": False, "type": type_label})
            master_df.to_sql("source_ids", conn, if_exists='append', index=False)
            
            for idx, id in enumerate(ids):
                print(f"Processing {type_label} ID {id} ({idx+1}/{len(ids)})...")
                
                try:
                    # Get the data depending on whether it's a builder or umbrella
                    if is_umbrella:
                        data = self.get_umbrella_detail(id)
                    else:
                        data = self.get_builder_detail(id)
                    
                    # Process each data type and save to a table
                    for key, value in data.items():
                        if value:
                            try:
                                # Convert to DataFrame and add source_id
                                if isinstance(value, list):
                                    # Add ID to each record
                                    for item in value:
                                        if isinstance(item, dict):
                                            item['source_id'] = id
                                    df = pd.DataFrame(value)
                                elif isinstance(value, dict):
                                    value['source_id'] = id
                                    df = pd.DataFrame([value])
                                else:
                                    print(f"Skipping {key} for ID {id}: Unsupported data type")
                                    continue
                                
                                if not df.empty:
                                    # Create a clean table name
                                    table_name = f"{type_label}_{key}"
                                    table_name = ''.join(c if c.isalnum() else '_' for c in table_name)
                                    
                                    # Append to existing table or create new one
                                    df.to_sql(table_name, conn, if_exists='append', index=False)
                                    print(f"Saved {key} data for ID {id} to table '{table_name}'")
                            except Exception as e:
                                print(f"Error saving {key} for ID {id}: {str(e)}")
                    
                    # Mark this ID as processed
                    cursor = conn.cursor()
                    cursor.execute("UPDATE source_ids SET processed = 1 WHERE id = ?", (id,))
                    conn.commit()
                    
                except Exception as e:
                    print(f"Error processing ID {id}: {str(e)}")
            
        finally:
            conn.close()
        
        print(f"Master database saved as: {db_path}")
    #END NEW MASTER LIST SAVE
    #END NEW ALL
    def __fetch_url(self, item):
        """
        Helper function to perform concurrent HTTP GET requests.

        Parameters:
        - item (tuple): A tuple containing the key and the URL for the request.

        Returns:
        - A tuple containing the key and the JSON result of the request.
        """
        key, url = item
        params = {"id": self.ID}
        response = self.session.get(url, params=params)
        return key, response.json()

'''
#Example usage of added functionality:
def main():
    api = API()
    #Umbrella details:
    print(api.get_umbrella_detail('12456315'))
    # Save specific ID:
    details = api.get_umbrella_detail("12456315") 
    api.save_to_csv(details, "umbrella_details", "12456315")     # Creates: builder_details_B12345_summary.csv
    api.save_to_xlsx(details, "umbrella_details", "12456315")    # Creates: builder_details_B12345.xlsx
    api.save_to_sql(details, "umbrella_database", "12456315")    # Creates: builder_database_B12345.db

    # Master list:
    umbrella_ids = ["12456315", "12383733"]
    api.save_multiple_to_master_csv(umbrella_ids, is_umbrella=True)
    api.save_multiple_to_master_xlsx(umbrella_ids, is_umbrella=True)
    api.save_multiple_to_master_sql(umbrella_ids, is_umbrella=True, db_name="umbrella_master_test")

if __name__ == "__main__":
    main()
'''
