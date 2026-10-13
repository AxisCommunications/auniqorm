# Releasing aUniqORM

1. Ensure the target commit is on the protected release branch and all required
   checks pass.
2. Create and publish a GitHub release with a `v<major>.<minor>.<patch>` tag.
3. The publish workflow builds the source distribution and wheel, validates them
   with Twine, then publishes through the `pypi` trusted-publisher environment.
4. If a release workflow needs recovery, run the Publish workflow manually and
   supply the existing release tag.

Configure the PyPI trusted publisher before the first release with GitHub owner
`AxisCommunications`, repository `auniqorm`, workflow
`.github/workflows/publish.yml`, and environment `pypi`.