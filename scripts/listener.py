import sys, subprocess, click

def writeflush(dest, s):
    dest.write(s)
    dest.flush()

def get_headers(line):
    """
    Parse Supervisor message headers.
    """

    return dict([x.split(':') for x in line.split()])

def supervisor_events(stdin, stdout):
    """
    An event stream from Supervisor.
    """

    while True:

        writeflush(stdout, 'READY\n')

        line = stdin.readline()
        headers = get_headers(line)
        payload = stdin.read(int(headers['len']))
        yield payload

        writeflush(stdout, 'RESULT 2\nOK')

@click.command()
@click.argument('mode')
def main(mode):
    for payload in supervisor_events(sys.stdin, sys.stdout):
        if mode == 'state':
            writeflush(sys.stderr, payload + '\n')
        elif mode == 'mcaf':
            line0, data = payload.split('\n',1)
            headers = get_headers(line0)
            if headers['processname'] == mode:
                subprocess.call("""echo "%s" | mailx -s 'test' caseyjlaw@gmail.com""" % data, shell=True)

if __name__ == '__main__':
    main()
