#!c:\users\zalexander\desktop\book-recommender\flask\venv\scripts\python.exe
# EASY-INSTALL-ENTRY-SCRIPT: 'databricks-cli==0.11.0','console_scripts','databricks'
__requires__ = 'databricks-cli==0.11.0'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('databricks-cli==0.11.0', 'console_scripts', 'databricks')()
    )
