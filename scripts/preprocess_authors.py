#!/usr/bin/env python3

import sys
import re

# Values taken from historical reddit source code:
# https://github.com/reddit-archive/reddit/blob/master/r2/r2/lib/validator/validator.py#L1567-L1570
MIN_USERNAME_LENGTH = 3
MAX_USERNAME_LENGTH = 20
USER_REGEX = re.compile(r"\A[\w-]+\Z", re.UNICODE)


def _replace_none(val):
    if val == 'None':
        return ''
    return val


def main():
    line_regex = re.compile(r"^(\d+) (.*?) ([\d]+|None) ([\d]+|None) (\-?[\d]+|None) (\-?[\d]+|None)$", re.UNICODE)

    for line in sys.stdin:
        try:
            m = line_regex.match(line)
            userid = m.group(1)
            username = m.group(2)
            start_date = _replace_none(m.group(3))
            end_date = _replace_none(m.group(4))
            karma_post = _replace_none(m.group(5))
            karma_comment = _replace_none(m.group(6))

            # Determine if username is valid based.
            # 0 (invalid) or 1 (valid) will be added as an additional column.
            valid = USER_REGEX.match(username) is not None and MIN_USERNAME_LENGTH <= len(
                username) <= MAX_USERNAME_LENGTH

            sys.stdout.write(
                "|".join([userid, username, start_date, end_date, karma_post, karma_comment, str(int(valid))]) + '\n')

        except:
            sys.stderr.write(line)


if __name__ == "__main__":
    main()
