import subprocess, time, click, sys, logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@click.command()
def monitor():
    """ Runs loop to check 'rq info', but only prints when changes to queue occur.
    """

    output0 = ['']
    logger.info('Monitoring rqinfo...')
    while 1:
        try:
            output = subprocess.check_output(['rq', 'info']).split('\n\n')
            if output[:-1] != output0[:-1]:   # if new, excluding time stamp
                logger.info('\n\n'.join(output))   # print out new status
                output0 = output   # update current status
            time.sleep(2)
        except KeyboardInterrupt:
            logger.info('Escaping...')
            break

        sys.stdout.flush()  # get it to print frequently
