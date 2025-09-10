# EU Clinical Trials Register (EUCTR) Data Source Documentation

## Introduction

The EU Clinical Trials Register (EUCTR) is a publicly accessible database containing information on interventional clinical trials on medicines. It was launched on March 22, 2011, to provide transparency on the design, conduct, and results of clinical trials. The register is a key resource for researchers, healthcare professionals, patients, and the general public.

This document provides an overview of the EUCTR data source, which is relevant for users of ETL (Extract, Transform, Load) packages designed to work with this data.

## Data Scope

The EUCTR contains information on:

*   **Interventional clinical trials on medicines** conducted in the European Union (EU) or the European Economic Area (EEA) that started after **May 1, 2004**.
*   **Paediatric clinical trials** conducted outside the EU/EEA if they are part of a Paediatric Investigation Plan (PIP) or sponsored by a marketing authorisation holder for use in the paediatric population.
*   Older paediatric trials that were completed by January 26, 2007, and are covered by an EU marketing authorisation.

The register **does not** include:

*   Non-interventional clinical trials (observational studies).
*   Clinical trials for surgical procedures, medical devices, or psychotherapeutic procedures.
*   Trials where all investigator sites are outside the EU/EEA, unless they are part of an agreed PIP.

## Data Access

The EUCTR website is the public interface for the **EudraCT database**. EudraCT is the database used by national medicines regulators for data related to clinical trial protocols.

Data from the EUCTR can be accessed in the following ways:

*   **Web Portal:** The data can be searched and viewed directly on the [EU Clinical Trials Register website](https://www.clinicaltrialsregister.eu/).
*   **Data Download:** The register allows for the download of up to **20 trial results at a time** in a text file (.txt) format. This is the primary method for extracting structured data for ETL processes.

## Data Structure

The EUCTR provides two main categories of information for each trial: protocol information and results.

### Protocol Information

The protocol-related information includes details about the trial's design and setup:

*   **Trial Design:** The design of the trial (e.g., randomized, double-blind).
*   **Sponsor:** The company or organization responsible for the clinical trial.
*   **Investigational Medicine:** The trade name or active substance of the medicine being studied.
*   **Therapeutic Areas:** The medical conditions being investigated.
*   **Trial Status:** The current status of the trial (e.g., authorised, ongoing, complete).

### Trial Results

The summary results for a trial include the following datasets:

*   **Trial Information:** General information about the trial.
*   **Subject Disposition:** Information on the flow of participants through the trial.
*   **Baseline Characteristics:** Demographic and baseline characteristics of the trial participants.
*   **Endpoints:** The primary and secondary outcomes of the trial.
*   **Adverse Events:** Information on adverse events that occurred during the trial.
*   **Additional Information:** Any other relevant information.
*   **Summary Attachment(s):** Attached documents with summary results.

## Clinical Trials Information System (CTIS)

The **Clinical Trials Information System (CTIS)** is the new, single-entry point for submitting and maintaining clinical trial information in the EU/EEA. It was launched on **January 31, 2022**, and it implements the Clinical Trials Regulation (EU) No 536/2014.

*   For clinical trials initiated **before January 31, 2023**, under the previous Clinical Trials Directive, the information is available in the **EU Clinical Trials Register (EUCTR)**.
*   For clinical trials initiated **on or after January 31, 2023**, the information is submitted to and available in **CTIS**.

Therefore, for comprehensive data coverage, an ETL process may need to extract data from both the EUCTR and the new CTIS public portal.

## Legal Basis

The legal basis for the EUCTR and the EudraCT database is provided by:

*   **Clinical Trial Directive 2001/20/EC**
*   **Regulation (EC) No 726/2004**
*   **Paediatric Regulation (EC) No 1901/2006**

These regulations mandate the public disclosure of clinical trial information.

## Source of Information

The information in the EUCTR is provided by the sponsors of the clinical trials and the national competent authorities of the EU/EEA member states. The European Medicines Agency (EMA) maintains the register but is not responsible for the accuracy or completeness of the information.
