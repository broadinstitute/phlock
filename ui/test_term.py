__author__ = 'pmontgom'


import term

tm = term.TerminalManager()
tm.start_periodic("title", lambda: ["bash", "-c", "sleep 3"], 1)


