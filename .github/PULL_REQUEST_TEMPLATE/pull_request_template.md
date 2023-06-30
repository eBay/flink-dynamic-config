## Contributing
Any changes to the repository are only accepted via [Pull Request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests):

Steps to contribute via the pull request:
1. [Fork the repository](https://help.github.com/articles/fork-a-repo/)
2. Make the changes with the helpful information in [README](README.md) to the forked repository
3. After the changes, run the tests against the forked repository
   ```sh
   mvn test
   ```
4. Commit and push the changes to your remote forked repository
5. Send the pull request to the official repository and describe the details of the changes
6. Wait for the committer to merge the changes into **main** branch and take care of the following CI build in
