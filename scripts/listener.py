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
        payload = payload.split('\n')
        yield payload[0], '\n'.join(payload[1:])
        writeflush(stdout, 'RESULT 2\nOK')

email_prefix = """Message for you from realfast... \n\n ====================================\n\n"""
email_suffix = """\n\n====================================\n\n"""

@click.command()
@click.option('--process', '-p', default='', help='Name of process to select. Default is all.')
@click.option('--select', '-s', default='', help='Select lines containing this string. Default is all.')
@click.option('--destination', '-d', default='log', help='Send lines to either \'stderr\' or \'email\'. Default is log (which is routed via stderr)')
@click.option('--addresses', '-a', default='caseyjlaw@gmail.com', help='If destination=email, where to send event? (comma-separated list)')
def main(process, select, destination, addresses):
    for ehead, edata in supervisor_events(sys.stdin, sys.stdout):
        ehead_parsed = get_headers(ehead)

        if process in ehead_parsed['processname']:    # select process from head
            if select in edata:     # select by string in edata
                if not len(edata):                # state event has no data. ehead becomes edata
                    edata = ehead   
                    
                # send data along
                if destination == 'log':
                    writeflush(sys.stderr, edata + '\n')
                elif destination == 'email' and addresses:
                    subprocess.call("""echo "%s" | mailx -s 'Supervisor Event (process %s and select %s)' %s""" % (email_prefix+str(edata)+email_suffix, process, select, addresses), shell=True)

if __name__ == '__main__':
    main()
