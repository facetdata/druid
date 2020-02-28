HOW-TOs
=========

Note - It might be a good idea to disable pushes to Apache Druid to prevent accidental leaks from `facet-druid` repo. 
Lets say the remote name for Apache Druid is `upstream` then running the follwing will disable pushes to it from the 
current repo - `git remote set-url --push upstream no_push` 

Add new facet specific extension
----------------
1. Checkout a new branch say `new_extension` from the default branch of this repo.
2. Develop `new_extension` in the new branch under `extensions-facet` folder, see `inspector` extension for example.
3. Add the extension module to parent `pom.xml`, by adding a `module` in `modules` section, like - 
`<module>extensions-facet/new_extension</module>`.
4. Add the new extension to the distribution list by adding it to `distribution/pom.xml` file under the `bundle-facet-exts`
profile, see `inspector` example in that file i.e.
```
    <argument>-c</argument>
    <argument>com.facetdata:inspector</argument>
``` 
5. Add the extension to global env var `FACET_EXTENSIONS` and `EXCLUDE_FACET_EXTENSIONS` as well as local env var `MAVEN_PROJECTS`
of job named `other modules test` in `.travis.yml` file. See `extensions-facet/inspector` for example.  
6. Quickly build/test the extension using `mvn clean test -pl extensions-facet/new_extension`.

Note - For Facet extensions `Header` and `PackageName` checks has been suppressed by adding `<suppress checks="Header" files="[\\/]extensions-facet[\\/]" />`and 
`<suppress checks="PackageName" files="[\\/]extensions-facet[\\/]" />` to the `checkstyle-suppressions.xml` file in the 
`codestyle` folder. Apache Rat checks are also not run for facet.

Upgrade to newer Apache Druid version
--------
Note - Here `upstream` points to `git@github.com:apache/incubator-druid.git` and `origin` points to `git@github.com:<company>/facet-druid.git`.
We are using `rebase` instead of `merge` because we want to replay our facet commits on top of Apache Druid and not merge
the histories, also with `rebase` we can resolve conflits one by one for each commit.

1. Checkout the branch from community that we want to upgrade to - `git checkout upstream/<latest_version>-incubating`.
2. Create a new branch from it for the current repo - `git checkout -b <latest_version>-facet`.
3. Push the new branch to the current repo - `git push origin <latest_version>-facet`.
4. Checkout previous branch of the current repo that has our custom commits - `git checkout <previous_version>-facet`.
5. Create one more branch upon which the current repo commits will be rebased - `git checkout -b <latest_version>-facet-rebase`.
5. Rebase the previous branch commits on top of new branch - `git rebase -i upstream/<latest_version>-incubating` and resolve the conflicts.
6. Upgrade the parent version of druid in facet extensions to match the version in parent pom and commit the changes.
7. Push the changes - `git push origin <latest_version>-facet-rebase`.
8. Create a PR against `<latest_version>-facet` branch and after merge make this the default branch.

Travis PR Builds
========
By default Travis will only build and test Facet specific modules on raising a PR. Travis will build/test entire Facet 
Druid codebase only when the branch name starts either with `backport` or `run_full_build` or ends with `facet-rebase`.

Docker Image Release
=======
1. Create a git tag on `branch =~ /^master|^.*-facet$/`, git tag would be following CalVer `date +"%y.%m.%d"` eg. 19.12.06.
1. Travis will build and push a docker image tag to GCR.
1. Travis will build and push a docker image tag on each merge with `${BRANCH}-${GIT_SHA}`

Web Console Development
========
See [web-console#apache-druid-web-console](web-console#apache-druid-web-console)

Deploying requires a full build of druid. Production JS compilation happens with Maven ([configuration](https://github.com/<company>/facet-druid/blob/0.16.0-facet/web-console/pom.xml))
