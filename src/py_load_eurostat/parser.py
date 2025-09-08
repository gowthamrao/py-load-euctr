from lxml import etree as ET
from typing import Dict, List


def parse_toc_xml(xml_content: str) -> List[Dict[str, str]]:
    """
    Parses the Table of Contents XML to extract dataset information.
    """
    datasets = []
    xml_bytes = xml_content.encode("utf-8")
    parser = ET.XMLParser(recover=True)
    root = ET.fromstring(xml_bytes, parser=parser)

    if root is None:
        return []

    # Define the correct namespace used in the XML
    ns = {"nt": "urn:eu.europa.ec.eurostat.navtree"}

    # Find all 'leaf' nodes, which represent datasets, using the namespace
    for leaf in root.findall(".//nt:leaf", namespaces=ns):
        try:
            # All children are also in the 'nt' namespace
            code_element = leaf.find("nt:code", namespaces=ns)
            title_element = leaf.find("nt:title[@language='en']", namespaces=ns)

            if code_element is not None and title_element is not None:
                code = code_element.text
                title = title_element.text

                # Construct the download URL based on the standard API format.
                download_url = f"https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{code}?format=TSV&compressed=true"

                datasets.append({"code": code, "title": title, "url": download_url})
        except AttributeError:
            continue

    return datasets


def get_dataset_download_url(
    datasets: List[Dict[str, str]], dataset_code: str
) -> str | None:
    """
    Finds the download URL for a specific dataset code.
    """
    for dataset in datasets:
        if dataset["code"] == dataset_code:
            return dataset["url"]
    return None
