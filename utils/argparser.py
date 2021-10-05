import argparse

# Create the parser
parser = argparse.ArgumentParser()
# Add argument to differenciate between local executions
parser.add_argument('--local', dest='local', action='store_true')
parser.add_argument('--no-local', dest='local', action='store_false')
parser.set_defaults(local=True)

# Add argument to point to testing tables
parser.add_argument('--testing', dest='testing', action='store_true')
parser.add_argument('--no-testing', dest='testing', action='store_false')
parser.set_defaults(testing=True)

# Add argument to define env_vars to use
parser.add_argument('--env_vars', type=str, required=False, default=None) # relative path


# Add argument to determine if to check last block in target BQ table
parser.add_argument('--check_last_block', dest='check_last_block', action='store_true')
parser.add_argument('--no-check_last_block', dest='check_last_block', action='store_false')
parser.set_defaults(check_last_block=False)

args = parser.parse_args()
print("args: ", args)