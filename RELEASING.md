## Instructions for releasing bigtable-1.x

### Install releasetool

See [releasetool](https://github.com/googleapis/releasetool) for installation instructions. You will
need python 3.6+ to run this tool.

## Prepare a release

1. Make sure the team agrees that it is time to release.

2. Run `mvn clean` on your branch.

3. Run `releasetool start`. Select "minor" or "patch" for the release type. This will bump the
   artifact versions, ask you to edit release notes, and create the release pull request. 
   You can either edit the release notes in your editor or wait until a PR is created on GitHub to edit there.

  **Note:** be sure to make these notes nice as they will be used for the release notes as well.

## Tag the release

1. Run `releasetool tag` to publish a release on Github. It will list the last few merged PRs.
   Select the newly merged release PR. Releasetool will create the GitHub release with notes
   extracted from the pull request and tag the new release.

## Pushing a release to Maven using Kokoro

1. Trigger the `java-bigtable-hbase/release/stage` Kokoro job using the tag as the commitish (ie `v.1.17.0`) and wait for it to complete. This will
   stage the built artifacts and prepare them for publishing.

2. Look through the logs for the `java-bigtable-hbase/release/stage` and find the staging repository
   ids used. It will look like `comgooglecloudbigtable-1234`.
   
3. **Optional** Check https://oss.sonatype.org/ to see the staged release.

4. Promote or drop the staged repository.

Be sure to again use the tag as the commitish (ie `v.1.17.0`)

   a. To publish the staged repository, trigger the `java-bigtable-hbase/release/promote` Kokoro job for
     each staging repository. To specify the staging repository, add an environment variable
     configuration with `STAGING_REPOSITORY_ID=<staging repository id>` from the UI.

   b. To drop (abort) the staged repository, trigger the `java-bigtable-hbase/release/drop` Kokoro job
     with the same staging repository id configuration as if you were publishing.
     
## Prepare the next snapshot version

1. Switch back to the branch you were releasing on (ie `bigtable-1.x`). Update your local branch (to merge in the version bump).

2. Run `releasetool start` to bump the next snapshot version. Select "snapshot" when prompted for
   the release type. This will bump the artifact versions and create a pull request.

3. Review and submit the PR.
