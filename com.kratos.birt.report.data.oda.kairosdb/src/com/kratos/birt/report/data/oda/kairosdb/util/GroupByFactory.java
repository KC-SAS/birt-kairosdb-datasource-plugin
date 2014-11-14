package com.kratos.birt.report.data.oda.kairosdb.util;

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



import java.util.HashMap;
import java.util.Map;

public class GroupByFactory 
{
	private Map<String, GroupBy> groupBys = new HashMap<String, GroupBy>();

	public GroupByFactory()
	{
		groupBys.put("tag", new TagGroupBy());
		groupBys.put("time", new TimeGroupBy());
		groupBys.put("value", new ValueGroupBy());

	}


	public GroupBy createGroupBy(String name)
	{
		return groupBys.get(name);
	}
}