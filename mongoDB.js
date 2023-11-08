// mongodb assignment
db.Title.insertMany([
    { "WORKER_REF_ID": 1, "WORKER_TITLE": "Manager", "AFFECTED_FROM": new Date("2023-02-20 00:00:00") },
    { "WORKER_REF_ID": 2, "WORKER_TITLE": "Executive", "AFFECTED_FROM": new Date("2023-06-11 00:00:00") },
    { "WORKER_REF_ID": 8, "WORKER_TITLE": "Executive", "AFFECTED_FROM": new Date("2023-06-11 00:00:00") },
    { "WORKER_REF_ID": 5, "WORKER_TITLE": "Manager", "AFFECTED_FROM": new Date("2023-06-11 00:00:00") },
    { "WORKER_REF_ID": 4, "WORKER_TITLE": "Asst.Manager", "AFFECTED_FROM": new Date("2023-06-11 00:00:00") },
    { "WORKER_REF_ID": 7, "WORKER_TITLE": "Executive", "AFFECTED_FROM": new Date("2023-06-11 00:00:00") },
    { "WORKER_REF_ID": 6, "WORKER_TITLE": "Lead", "AFFECTED_FROM": new Date("2023-06-11 00:00:00") },
    { "WORKER_REF_ID": 3, "WORKER_TITLE": "Lead", "AFFECTED_FROM": new Date("2023-06-11 00:00:00") }
])

const Worker = db.Worker.insertMany([
    { "WORKER_ID": "001", "FIRST_NAME": "Monika", "LAST_NAME": "Arora", "SALARY": 100000, "JOINING_DATE": new Date("2021-02-20 09:00:00"), "DEPARTMENT": "HR" },
    { "WORKER_ID": "002", "FIRST_NAME": "Niharika", "LAST_NAME": "Verma", "SALARY": 80000, "JOINING_DATE": new Date("2021-06-11 09:00:00"), "DEPARTMENT": "Admin" },
    { "WORKER_ID": "003", "FIRST_NAME": "Vishal", "LAST_NAME": "Singhal", "SALARY": 300000, "JOINING_DATE": new Date("2021-02-20 09:00:00"), "DEPARTMENT": "HR" },
    { "WORKER_ID": "004", "FIRST_NAME": "Amitabh", "LAST_NAME": "singh", "SALARY": 500000, "JOINING_DATE": new Date("2021-02-20 09:00:00"), "DEPARTMENT": "Admin" },
    { "WORKER_ID": "005", "FIRST_NAME": "Vivek", "LAST_NAME": "Bhati", "SALARY": 500000, "JOINING_DATE": new Date("2021-06-11 09:00:00"), "DEPARTMENT": "Admin" },
    { "WORKER_ID": "006", "FIRST_NAME": "Vipul", "LAST_NAME": "Diwan", "SALARY": 200000, "JOINING_DATE": new Date("2021-06-11 09:00:00"), "DEPARTMENT": "Account" },
    { "WORKER_ID": "007", "FIRST_NAME": "Satish", "LAST_NAME": "Kumar", "SALARY": 75000, "JOINING_DATE": new Date("2021-01-20 09:00:00"), "DEPARTMENT": "Account" },
    { "WORKER_ID": "008", "FIRST_NAME": "Geetika", "LAST_NAME": "Chauhan", "SALARY": 90000, "JOINING_DATE": new Date("2021-04-11 09:00:00"), "DEPARTMENT": "Admin" },
])

const Title = db.Title.insertMany([
    { "WORKER_REF_ID": 1, "WORKER_TITLE": "Manager", "AFFECTED_FROM": new Date("2023-02-20 00:00:00") },
    { "WORKER_REF_ID": 2, "WORKER_TITLE": "Executive", "AFFECTED_FROM": new Date("2023-06-11 00:00:00") },
    { "WORKER_REF_ID": 8, "WORKER_TITLE": "Executive", "AFFECTED_FROM": new Date("2023-06-11 00:00:00") },
    { "WORKER_REF_ID": 5, "WORKER_TITLE": "Manager", "AFFECTED_FROM": new Date("2023-06-11 00:00:00") }, ,
    { "WORKER_REF_ID": 4, "WORKER_TITLE": "Asst.Manager", "AFFECTED_FROM": new Date("2023-06-11 00:00:00") },
    { "WORKER_REF_ID": 7, "WORKER_TITLE": "Executive", "AFFECTED_FROM": new Date("2023-06-11 00:00:00") },
    { "WORKER_REF_ID": 6, "WORKER_TITLE": "Lead", "AFFECTED_FROM": new Date("2023-06-11 00:00:00") },
    { "WORKER_REF_ID": 3, "WORKER_TITLE": "Lead", "AFFECTED_FROM": new Date("2023-06-11 00:00:00") },
])
const A = db.Worker.aggregate([
    { $project: { _id: 0, WORKER_NAME: "$FIRST_NAME" } }
])
const B = db.Worker.aggregate([
    {
        $project: { _id: 0, FIRST_NAME: { $toUpper: "$FIRST_NAME" } }
    }
])
const C = db.Worker.distinct("DEPARTMENT")

const D = db.Worker.aggregate([{
    $project: { _id: 0, NAME: { $substr: ["$FIRST_NAME", 0, 3] } }
}])

const F = db.Worker.aggregate([{
    $project: { _id: 0, Position: { $indexOfCP: ["$FIRST_NAME", "a"] } }
}])

const G = db.Worker.aggregate([{
    $project: { _id: 0, FIRST_NAME_SPACEREMOVED: { $rtrim: { input: "$FIRST_NAME" } } }
}])


const H = db.Worker.aggregate([{
    $project: { _id: 0, FIRST_NAME_SPACEREMOVED: { $ltrim: { input: "$FIRST_NAME" } } }
}])

const I = db.Worker.distinct("DEPARTMENT").length

const J = db.Worker.aggregate([
    {
        $project:
        {
            _id: 0, Replaced_FIRST_NAME: { $replaceOne: { input: "$FIRST_NAME", find: "A", replacement: "a" } }
        }
    }
])

const K = db.Worker.aggregate(
    [
        { $project: { _id: 0, FULLNAME: { $concat: ["$FIRST_NAME", " ", "$LAST_NAME"] } } }
    ]
)

const L = db.Worker.find().sort({ FIRST_NAME: 1 })

const M = db.Worker.find().sort({ FIRST_NAME: 1, DEPARTMENT: -1 })

const N = db.Worker.find({ FIRST_NAME: { $in: ["Vipul", "Satish"] } }) //13

const O = db.Worker.find({ FIRST_NAME: { $nin: ["Vipul", "Satish"] } }) ///14

const P = db.Worker.find({ DEPARTMENT: "Admin" }) //15

const Q = db.Worker.find({ FIRST_NAME: { $regex: 'a', $options: 'i' } })//16

const R = db.Worker.find({ FIRST_NAME: { $regex: 'a$', $options: 'i' } })//17

const S = db.Worker.find({ FIRST_NAME: { $regex: 'h$', $options: 'i' } })//18

const T = db.Worker.find({ SALARY: { $gt: 100000, $lt: 500000 } })

const U = db.Worker.find({ JOINING_DATE: { $gte: new Date("2021-02-20"), $lt: new Date("2021-03-01") } })//20


const V = db.Worker.find({ DEPARTMENT: "Admin" }).count()

const workersName = db.Worker.find({ SALARY: { $gt: 100000, $lt: 500000 } }, { _id: 0, FULLNAME: { $concat: ["$FIRST_NAME", " ", "$LAST_NAME"] }, SALARY: 1 })//22

const workersforeachdepartment = db.Worker.aggregate([{
    $group: { _id: "$DEPARTMENT", count: { $sum: 1 } }
}, { $sort: { count: -1 } }])//23

const Managers = db.Worker.aggregate([
    { $project: { WORKER_ID: { $toInt: "$WORKER_ID" } } },
    {
        $lookup: {
            from: "Title",
            localField: "WORKER_ID",
            foreignField: "WORKER_REF_ID",
            as: "titles"
        }

    },
    {
        $match: {
            "titles.WORKER_TITLE": "Manager"
        }
    }
])//24

//  Write a query to fetch duplicate records having matching data in some fields of a table.
const duplicate = db.Worker.aggregate([
    {
        $group: {
            _id: { DEPARTMENT: "$DEPARTMENT" },
            count: { $sum: 1 },
        }
    },
    {
        $match:
        {
            count: { $gt: 1 }
        }
    }, {
        $lookup:
        {
            from: "Worker",
            localField: "_id.DEPARTMENT",
            foreignField: "DEPARTMENT",
            as: "datas"
        }
    }
]) //25

const oddFolder = db.Worker.find({ $expr: { $eq: [{ $mod: [{ $toInt: "$WORKER_ID" }, 2] }, 1] } }) //26

const EvenFolder = db.Worker.find({ $expr: { $eq: [{ $mod: [{ $toInt: "$WORKER_ID" }, 2] }, 0] } }) //27

const clone = db.Worker.aggregate([{ $out: "newTable" }])

const fetchDataFrmTwoTables = db.Bonus.aggregate([
    {
        $lookup: {
            from: "Title",
            localField: "WORKER_REF_ID",
            foreignField: "WORKER_REF_ID",
            as: "intersection"
        }
    }
]) //29
const DataNotHaveTable =
    db.Title.aggregate([
        {
            $lookup: {
                from: "Bonus",
                localField: "WORKER_REF_ID",
                foreignField: "WORKER_REF_ID",
                as: "bonusData"
            }
        },
        {
            $match: {
                bonusData: { $size: 0 }
            }
        }
    ])//30

db.Bonus.aggregate([
    {
        $project: {
            current_date: "$$NOW"
        }
    }
])//31

db.Worker.find().limit(5) //32

db.Worker.find().sort({
    SALARY: -1
}).skip(4).limit(1)//33

db.Worker.aggregate([
    {
        $sort: { SALARY: -1 }
    },
    {
        $group: {
            _id: null,
            salaries: { $addToSet: "$SALARY" }
        }
    },
    {
        $project: {
            fifthHighest: { $arrayElemAt: ["$salaries", 4] }
        }
    }
])//34

db.Worker.aggregate([
    {
        $group: {
            _id: "$SALARY",
            employees: { $push: { $concat: ["$FIRST_NAME", " ", "$LAST_NAME"] } },
            count: { $sum: 1 }
        }
    },
    {
        $match: {
            count: { $gt: 1 }
        }
    }
])//35

db.Worker.aggregate([
    {
        $group: {
            _id: null,
            salaries: { $addToSet: "$SALARY" }
        }
    }, {
        $sort: { salaries: -1 }
    }, {
        $project: {
            secondHighest: { $arrayElemAt: ["$salaries", 1] }
        }
    }
]) //36

//37
const fetchDataFrmTwoTabless = db.Bonus.aggregate([
    {
        $lookup: {
            from: "Title",
            localField: "WORKER_REF_ID",
            foreignField: "WORKER_REF_ID",
            as: "intersection"
        }
    }
])//38
db.Worker.find().limit(db.Worker.count() / 2)//39

db.Worker.aggregate([
    {
        $group: {
            _id: "$DEPARTMENT",
            count: { $sum: 1 }
        }
    },
    {
        $match: {
            count: { $lt: 5 }
        }
    }
])//40
db.Worker.aggregate([
    {
        $group: {
            _id: "$DEPARTMENT",
            count: { $sum: 1 }
        }
    }
]) //41
db.Worker.find().sort({ $natural: -1 }).limit(1)//42
db.Worker.find().limit(1) //43
db.Worker.find().sort({ $natural: -1 }).limit(5)//44
db.Worker.aggregate([
    {
        $sort: { SALARY: -1 }
    },
    {
        $group: {
            _id: "$DEPARTMENT",
            highestSalaryEmployee: { $first: "$FIRST_NAME" },
            highestSalary: { $first: "$SALARY" }
        }
    },
    {
        $project: {
            _id: 0,
            Department: "$_id",
            Employee: "$highestSalaryEmployee",
            Salary: "$highestSalary"
        }
    }
])//45
db.Worker.aggregate([
    { $group: { _id: "$SALARY" } },
    { $sort: { _id: -1 } },
    { $limit: 3 }
])//46
db.Worker.aggregate([
    { $group: { _id: "$SALARY" } },
    { $sort: { _id: 1 } },
    { $limit: 3 }
])//47
db.Worker.aggregate([
    { $group: { _id: "$SALARY" } },
    { $sort: { _id: -1 } },
    { $skip: 4 },
    { $limit: 1 },
    { $project: { _id: 0, saleries: "$_id" } }
])//48
db.Worker.aggregate([
    {
        $group: {
            _id: "$DEPARTMENT",
            totalSalaries: { $sum: "$SALARY" }
        }
    }
])//49
db.Worker.aggregate([
    { $sort: { SALARY: -1 } },
    { $limit: 1 },
    { $project: { _id: 0, FIRST_NAME: 1, LAST_NAME: 1 } }
])//50
