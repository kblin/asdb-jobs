import nox


@nox.session
def lint(session):
    session.install(".[testing]")
    session.run("pylint")
