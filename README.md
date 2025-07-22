# common-voice-utils
Utilities for working with the Common Voice dataset

Developed with Python 3.9.6

The scripts in this repo can be used for:
* scraping the download URLs and downloading each lanaguage dataset from the Common Voice website (https://commonvoice.mozilla.org/en/datasets)
* Creating symlinks to the validated audio files by reading the `validated.tsv` file and matching it with the corrisponding audio file in directory `clips`
* Renaming the validated TSV files (just housekeeping really)

Modifications and comments welcome.

## Setup
This project requires Python 3.8 or newer.

This project uses webdriver-manager to handle Chromedriver installation automatically.

No manual Chromedriver download required!

### Installation
1. Clone this repository
2. Install dependencies: `pip install -r requirements.txt`
