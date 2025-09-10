# EU Clinical Trials Register (EUCTR) Data Source Documentation

This document provides an overview of the EU Clinical Trials Register (EUCTR) as a data source, which is relevant for users of this ETL package.

## What is the EU Clinical Trials Register (EUCTR)?

The EU Clinical Trials Register (EUCTR) is a database that provides public access to information on interventional clinical trials on medicines conducted in the European Union (EU) and the European Economic Area (EEA). It also includes information on some trials conducted outside the EU/EEA, particularly those related to paediatric medicine.

The register was launched to increase transparency in clinical research and provides information from the EudraCT database.

## Important Note: EUCTR vs. CTIS

As of **January 31, 2022**, a new system called the **Clinical Trials Information System (CTIS)** was launched. This has led to a separation of where clinical trial data is stored:

*   **EUCTR**: Contains information on clinical trials initiated **before** January 31, 2022.
*   **CTIS**: Contains information on clinical trials initiated **on or after** January 31, 2022.

This ETL package is designed to work with the **EUCTR**. For data on trials started after the cut-off date, you will need to consult the CTIS.

## Data Content

The EUCTR contains a wide range of information about clinical trials, including:

*   **Protocol Information**:
    *   Trial design and status (authorised, ongoing, complete)
    *   Sponsor details
    *   Investigational medicinal product information
    *   Therapeutic areas of the trial
*   **Results Information**:
    *   Trial subject disposition (how many subjects started and completed the trial)
    *   Baseline characteristics of the trial population
    *   Endpoints (the main outcomes measured in the trial)
    *   Adverse events reported during the trial

## Data Source and Quality

The data in the EUCTR is provided by the trial sponsors and national competent authorities in the EU/EEA countries.

**Important**: The European Medicines Agency (EMA), which maintains the register, is not responsible for the completeness or accuracy of the information. There may be data quality issues, and users should be cautious when interpreting the data.

## Accessing the Data

The EUCTR website allows for manual searching and viewing of trial information. It also provides a feature to download up to 20 trial results at a time as a text file. This ETL package is designed to automate the process of extracting this data on a larger scale.

## Key Identifiers

The primary identifier for a clinical trial in this database is the **EudraCT Number**. This is a unique number assigned to each trial when it is entered into the EudraCT database.

## What is NOT in the EUCTR?

The EUCTR **does not** provide information on:

*   Non-interventional clinical trials (observational studies).
*   Clinical trials for surgical procedures, medical devices, or psychotherapeutic procedures.
*   Trials where all investigator sites are outside the EU/EEA (with some exceptions for paediatric trials).
*   Authorisation documents from national regulators or ethics committee opinions.
