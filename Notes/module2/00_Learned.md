# What I learned

# The Flow code
Take for instance:
```
if __name__ == "__main__":
    months = [1,2]
    year = 2020
    color = "green"
    do_flow_code(months, year, color)
```
The flow code is `do_flow_code(...)`.
That will be the entry point of the deployment.
So _months_, _year_ and _color_ are parameters stored as default.
So take for instance if you built the flow already and decided to change the defaults.
It wouldn't take effect because those _defaults_ were created when building.
When running deployment, use the -p flag to change any parameters.

But this is about all I know so far for certain.


## LINKS
https://towardsdatascience.com/create-robust-data-pipelines-with-prefect-docker-and-github-12b231ca6ed2
