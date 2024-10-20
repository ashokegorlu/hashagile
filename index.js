const XLSX = require("xlsx");
const { Client } = require("@elastic/elasticsearch");

// Initialize the Elasticsearch client
const client = new Client({ node: "http://localhost:9200" });

// Set the correct file path to your Excel file
const filePath = "D:/pro-elasticsearch/Employee Sample Data 1.xlsx";

// Function to create a new collection (index)
const createCollection = async (collectionName) => {
  try {
    await client.indices.create({ index: collectionName });
    console.log(`Created collection: ${collectionName}`);
  } catch (error) {
    console.error(`Error creating collection: ${error.meta.body.error.reason}`);
  }
};

// Function to get the employee count from a collection
const getEmpCount = async (collectionName) => {
  try {
    const { body } = await client.count({ index: collectionName });
    console.log(`Employee count in ${collectionName}: ${body.count}`);
  } catch (error) {
    console.error(
      `Error getting employee count for ${collectionName}: ${error.message}`
    );
  }
};

// Function to index data into a collection
const indexData = async (collectionName, data) => {
  await client.index({
    index: collectionName,
    body: data,
  });
  console.log(`Indexed data in ${collectionName}`);
};

// Function to index data from an Excel file
const indexDataFromExcel = async (collectionName) => {
  const workbook = XLSX.readFile(filePath); // Read the Excel file
  const sheetName = workbook.SheetNames[0]; // Get the first sheet
  const worksheet = workbook.Sheets[sheetName];
  const data = XLSX.utils.sheet_to_json(worksheet); // Convert to JSON

  for (const record of data) {
    await indexData(collectionName, record); // Index each record
  }
  console.log(`Indexed data from ${filePath} into ${collectionName}`);
};

// Function to delete an employee by ID
const delEmpById = async (collectionName, id) => {
  await client.delete({ index: collectionName, id: id });
  console.log(`Deleted employee with ID ${id} from ${collectionName}`);
};

// Function to search for a column value
const searchByColumn = async (collectionName, column, value) => {
  const { body } = await client.search({
    index: collectionName,
    body: {
      query: {
        match: { [column]: value },
      },
    },
  });
  console.log(`Search results for ${column}=${value}:`, body.hits.hits);
};

// Function to execute the series of operations
const executeFunctions = async () => {
  const v_nameCollection = "krishna"; // Your name
  const v_phoneCollection = "2885"; // Your phone's last four digits

  await createCollection(v_nameCollection);
  await createCollection(v_phoneCollection);
  await getEmpCount(v_nameCollection);
  await indexDataFromExcel(v_nameCollection); // Index data from Excel
  await indexData(v_phoneCollection, { Gender: "Male" }); // Adjust if necessary
  await getEmpCount(v_nameCollection);
  await delEmpById(v_nameCollection, "E02003"); // Change ID as necessary
  await getEmpCount(v_nameCollection);
  await searchByColumn(v_nameCollection, "Department", "IT"); // Adjust search parameters if needed
  await searchByColumn(v_nameCollection, "Gender", "Male"); // Adjust search parameters if needed
  await searchByColumn(v_phoneCollection, "Department", "IT"); // Adjust search parameters if needed
};

// Start executing the functions
executeFunctions().catch(console.error);
