// MongoDB Playground
// Use Ctrl+Space inside a snippet or a string literal to trigger completions.

// The current database to use.
use("aggregation_pipeline");

//1. Find all users who are active
db.users.aggregate([
  {
    $match: {
      isActive: true,
    },
  },
  {
    $count: "activeUsers",
  },
]);

//2. Find average age of a user

// 2.1 Find all in 1 single group.
/**
 * Sample Output: {
		"_id": null,
		"averageAge": 29.835
	}
 */
db.users.aggregate([
  {
    $group: {
      _id: null,
      averageAge: {
        $avg: "$age",
      },
    },
  },
]);

// 2.2 Group based on fields. For example: gender
/**
 * Sample Output: [
 * 	{
			"_id": "female",
			"averageAge": 29.81854043392505
		},
		{
			"_id": "male",
			"averageAge": 29.851926977687626
		}
 * ]
 */
db.users.aggregate([
  {
    $group: {
      _id: "$gender",
      averageAge: {
        $avg: "$age",
      },
    },
  },
]);

//3. List top 5 common fruits among user
db.users.aggregate([
  {
    $group: {
      _id: "$favoriteFruit",
      count: {
        // $count: {}
        $sum: 1,
      },
    },
  },
  {
    $sort: {
      count: -1,
    },
  },
  {
    $limit: 5,
  },
]);

// 4. Find total number of males and females
db.users.aggregate([
  {
    $group: {
      _id: "$gender",
      genderCount: {
        $sum: 1,
      },
    },
  },
]);

//5. List top 2 countries has highest number of registered users
db.users.aggregate([
  {
    $group: {
      _id: "$company.location.country",
      userCount: {
        $sum: 1,
      },
    },
  },
  {
    $sort: {
      userCount: -1,
    },
  },
  {
    $limit: 2,
  },
]);

// 6. List all unique eye colours in the collection
db.users.aggregate([
  {
    $group: {
      _id: "$eyeColor",
    },
  },
]);

//7. What is the average number of tags per user
db.users.aggregate([
  /**
   * Approach 1: We use `unwind` operator, which destructures elements of array and makes new document per element.
   * Then we will calculate number of tags per user by grouping based on id -> Equivalent operation to tags.length in JS
   */
  // {
  // 	$unwind: {
  // 		path: "$tags"
  // 	} // OR $unwind: "$tags" for single paths
  // },
  // {
  // 	$group: {
  // 		_id: "$_id",
  // 		numberOfTags: {
  // 			$sum: 1
  // 		}
  // 	}
  // },
  /**
   * Approach 2: We use `addFields` operator which directly adds a new field. In our case we add `numberOfTags`
   * and we compute total numbers of tags per user using `size` operator -> Equivalent operation to tags.length in JS
   */
  {
    $addFields: {
      numberOfTags: {
        $size: {
          $ifNull: ["tags", []], // This basically computes based on tags array and if not present, or is null, then treat it as an empty array (so that length for the same can be read as 0)
        },
      },
    },
  },
  {
    $group: {
      _id: null,
      tagsAverage: {
        $avg: "$numberOfTags",
      },
    },
  },
]);

//8. How many users have a specific tag associated with them? For example 'enim' -> Equivalent of JS filter
db.users.aggregate([
  {
    $match: {
      tags: "enim",
    },
  },
  {
    $count: "enimTag",
  },
]);

//9. Names and age of users who are inactive, and have `velit` as tag
db.users.aggregate([
  {
    $match: {
      isActive: false,
      tags: "velit",
    },
  },
  {
    $project: {
      name: true,
      age: true,
      _id: false,
    },
  },
]);

//10. How many users have a phone number starting with `+1 (940)`
db.users.aggregate([
  {
    $match: {
      "company.phone": /^\+1 \(940\)/,
    },
  },
  {
    $count: "specialPhoneUser",
  },
]);

//11. List 5 most recent registered users
db.users.aggregate([
  {
    $sort: {
      registered: -1,
    },
  },
  {
    $limit: 5,
  },
  {
    $project: {
      name: true,
      registered: true,
      favoriteFruit: true,
    },
  },
]);

//12. Categorize users by their favouriteFruit
db.users.aggregate([
  {
    $group: {
      _id: "$favoriteFruit",
      users: {
        $push: "$name",
      },
    },
  },
]);

//13. List users having `ad` as their second tag in their tags list
db.users.aggregate([
  {
    $match: {
      "tags.1": "ad",
    },
  },
  {
    $count: "secondTagAd",
  },
]);

//14. List users who have both `enim` and `id` in their tags
db.users.aggregate([
  {
    $match: {
      tags: {
        $all: ["enim", "id"],
      },
    },
  },
]);

//15. List all companies located in USA with their corresponding user count
db.users.aggregate([
  {
    $match: {
      "company.location.country": "USA",
    },
  },
  {
    $group: {
      _id: "$company.title",
      usersCount: {
        $sum: 1,
      },
    },
  },
]);

//16. Fetch author details of a book
/**
 * Sample Output: [
 *  {
      "genre": "Classic",
      "author_details": {
        "_id": 100,
        "name": "F. Scott Fitzgerald",
        "birth_year": 1896
      },
      "_id": 1,
      "title": "The Great Gatsby",
      "author_id": 100
    }
 * ]
 */
db.books.aggregate([
  {
    $lookup: {
      from: "authors",
      localField: "author_id",
      foreignField: "_id",
      as: "author_details",
    },
  },
  {
    $addFields: {
      author_details: {
        // $first: "$author_details",
        $arrayElemAt: ["$author_details", 0], // Since `author_details` in previous stage gives an array and we want the first value
      },
    },
  },
]);
