# Week 1: Code versioning & CI/CD

In this step, you shall set up your Github repository within which you will be doing your code.

## What you expect to learn:

1. Code versioning with Git
2. CI/CD pipelines - automating 
3. How to structure your code repositories

## Tasks:

1. Everyone should create a [github account](https://github.com/) if you do not have one already. 
2. Create a repository(one per group) and add your team members as contributors
3. Clone your repository to your local computers. (*Hint*: Working with git on your local computer requires [git](https://git-scm.com/downloads) or tools like [sourcetree](https://www.sourcetreeapp.com/))
4. Set up the following repository structure that will hold our project
    1. *Hint*: add `.gitkeep` to be able to commit and push empty folder - see [docs](https://dev.to/ritaly/git-lesson-how-to-use-gitignore-and-gitkeep-5edm)

        ```
        .
        ├── .github - will hold your github workflows
        ├── analysis - data analysis ( Week 2)
        └── dags - will hold your Airflow DAGs ( Week 3)
        ```
5. Create a PR to push the new repository structure to your repository
    1. *Hints*
        1. Create a branch
        2. Create a PR from your branch to the main/master branch
6. Create a simple Github Actions workflow that prints “hello world”. See resource section #2
    1. Use Free github hosted runners - [how-to](https://docs.github.com/en/actions/using-github-hosted-runners/about-github-hosted-runners/about-github-hosted-runners)


## Resources

1. Git Basics
    1. https://git-scm.com/book/en/v2/Git-Basics-Getting-a-Git-Repository
    2. https://rogerdudler.github.io/git-guide/
2. Github Actions
    1. https://docs.github.com/en/actions/learn-github-actions/understanding-github-actions