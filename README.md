circle_storage
=============

circle storage

# How to use it
- Write data:
circle_storage:write({Record_meta, Record_bin}),

# Sample rebar config
{deps, [
	{circle_storage, ".*",
		{git, "https://github.com/DennyZhang/circle_storage.git", {tag, "master"}}}
]}.