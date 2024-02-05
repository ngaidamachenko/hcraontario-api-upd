import requests
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
