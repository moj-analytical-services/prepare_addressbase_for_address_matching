
### Using a shebang to create an executable file

A shebang can be added to make a script executable without using `uv run` — this makes it easy to run scripts that are on your `PATH` or in the current folder.

For example, create a file called `greet` with the following contents

greet

`#!/usr/bin/env -S uv run --script

print("Hello, world!")
`

Ensure that your script is executable, e.g., with `chmod +x greet`, then run the script:

`$ ./greet
Hello, world!
`

Declaration of dependencies is also supported in this context, for example:

example

`#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx"]
# ///

import httpx

print(httpx.get("https://example.com"))