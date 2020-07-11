#!c:\users\zalexander\desktop\book-recommender\flask\venv\scripts\python.exe
# EASY-INSTALL-ENTRY-SCRIPT: 'databricks-connect==6.5.1','console_scripts','databricks-connect'
__requires__ = 'databricks-connect==6.5.1'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('databricks-connect==6.5.1', 'console_scripts', 'databricks-connect')()
    )
