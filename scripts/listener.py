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
@click.option('--process', '-p', default='', help='Name of process to select. Default is all.')
@click.option('--select', '-s', default='', help='Select lines containing this string. Default is all.')
@click.option('--destination', '-d', default='stderr', help='Send lines to either stderr or email. Default is stderr (which is saved as log file)')
def main(process, select, destination):
    for payload in supervisor_events(sys.stdin, sys.stdout):
        if destination == 'stderr' and process == 'state':
            writeflush(sys.stderr, payload + '\n')
        else:
            line0, data = payload.split('\n',1)
            headers = get_headers(line0)
            
            if process in headers['processname']:
                if select in data:
                    if destination == 'stderr':
                        writeflush(sys.stderr, payload + '\n')
                    elif destination == 'email':
                        subprocess.call("""echo "%s" | mailx -s 'test' caseyjlaw@gmail.com""" % data, shell=True)

if __name__ == '__main__':
    main()
