package com.kratos.birt.report.data.oda.kairosdb.json;

/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */


import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.apache.bval.constraints.NotEmpty;

public class TagGroupBy implements GroupBy
{
	@NotNull
	@NotEmpty()
	private List<String> tags;

	public TagGroupBy()
	{
	}

	public TagGroupBy(List<String> tagNames)
	{
		checkNotNull(tagNames);
		this.tags = new ArrayList<String>(tagNames);
	}

	public TagGroupBy(String... tagNames)
	{
		this.tags = new ArrayList<String>();
		Collections.addAll(this.tags, tagNames);
	}


	/**
	 * Returns the list of tag names to group by.
	 * @return list of tag names to group by
	 */
	public List<String> getTagNames()
	{
		return Collections.unmodifiableList(tags);
	}

	public void setTags(List<String> tags)
	{
		this.tags = tags;
	}

	@Override
	public void addToSet(Set<String> tagSet, Set<Integer> valueSet,
			Set<Duration> timeSet) {
		for(String tag : tags){
			tagSet.add(tag);
		}
		
	}
}