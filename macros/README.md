### Understanding metrics macros
If you're interested in writing your own metrics macro or are curious about how the sql is generated for your metrics query, you've come to the right place! This readme will contain information on the flow of the most important macros, the inputs needed to make them work, and short explanations of how they all work!

#### The Flow 
As of version v0.3.2, significant work has been done on breaking out logic into discrete and logical components to ensure that each macro always performs the same behavior, regardless of how or where it is called. To wit, the first metrics always called are either:

- **calculate**: this is the most frequently used macro by end-users and is documented well in the overarching README. 
- **develop**:  this macro allows users to provide metric yml and test/simulate what the end result would look like if said metric were included in their project

Once these macros are called, they both go through two logical steps albeit in slightly different ways.

- validation: Both macros validate that their inputs are correct and match what we are expecting to see. Additionally they also validate the inputs against the existing metrics object in the manifest to ensure that dimensions are correct, time grains are permitted, etc etc. 
- variable creation: Both macros also create 2 variables that are required in downstream processes. The `metric_tree` and the `metrics_dictionary`.

**Metric Tree**: This object is a dictionary that contains 5 key value pairs:
- full_set: this value is a list of **all** metric names that are required to construct the sql. It includes all parent metrics and experssion metrics.
- base_set: this value is a list of metric names that are provided to the macro. 
- parent_set: this value is a list of parent metric names, which are defined as all first level (non-derived) metrics upon which downstream metrics are dependent upon.
- derived_set: this value is a list of derived metric names
- ordered_derived_set: this value is a list of dictionaries that contains the derived metrics **and** their depth from the parent. This is used to construct the nested CTEs in the sql gen.

**Metrics Dictionary**: This object is a dictionary that contains all of the attributes for each metric in the full_set. It was implemented in v0.3.2 to support the same input provided to `get_metric_sql` from both develop and calculate. It must contain the:
- Metric name
- Metric calculation method
- Metric expression
- Metric timestamp
- Metric time grains
- Metric dimensions
- Metric filters
- (If not derived) Metric model name
- (If not derived) Metric model object