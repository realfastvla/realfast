from jinja2 import Environment

# stuff for jinja2 template for index to browse cand summary plots

HTML = """
<html>
<body>

 <ul>
 {% for item in contents %}
  <li><a href="{{ item[1] }}" target="frame">{{ item[0] }}</a> ({{ item[2] }} MB)</li>
 {% endfor %}
 </ul>

</body>
</html>
"""

def gethtmlcontents(directory):
    filenames = glob.glob(os.path.join(directory, 'cands*_merge.html'))
    mjds = ['.'.join(ff.rstrip('_merge.html').split('.')[-2:])
            for ff in filenames]
    sizes = [os.stat(ff).st_size/(1024**2)
             for ff in filenames]
    contents = zip(mjds, [os.path.basename(ff) for ff in filenames], sizes)
    return sorted(contents, key=lambda ff: ff[0])


def writehtml(directory, outname='contents.html'):
    contents = gethtmlcontents(directory)
    contentshtml = Environment().from_string(HTML).render(contents=contents, title='List of Contents')

    with open(os.path.join(directory, outname), 'w') as f:
        f.write(contentshtml)
