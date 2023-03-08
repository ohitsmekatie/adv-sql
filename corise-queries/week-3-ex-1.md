## Query Performance 

> In addition to your query, please submit a short description of what you determined from the query profile and how you structured your query to plan for a higher volume of events once the website traffic increases.

When first running my query I included a join at the very name to get the recipe name - which was outside the scope of the project. This actually ended up being the biggest performance hog in my query at first:

<img width="363" alt="Screen Shot 2023-02-18 at 8 04 29 AM" src="https://user-images.githubusercontent.com/9855295/219867836-76befd94-7370-427b-964e-4fb95a31d3a1.png">

so I decided to remove that from the final query since that information can be brought in later, if need be and that really improved my query speed:

<img width="369" alt="Screen Shot 2023-02-18 at 8 09 06 AM" src="https://user-images.githubusercontent.com/9855295/219867872-2a1c2514-176e-4983-95af-6960f347d4bd.png">

The most expensive node was my join and I think there's probably a way for this query to be re-worked to calculate the top recipe in the `calculate metrics` CTE. Perhaps in a windows function? That's something I would think of refactoring if performance of the query as is was not future proofed enough. 
