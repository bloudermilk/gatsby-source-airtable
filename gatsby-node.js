const Airtable = require("airtable");
const crypto = require(`crypto`);
const { createRemoteFileNode } = require(`gatsby-source-filesystem`);
const { map } = require("bluebird");

const DIRTY_WARNING = `Field names within graphql cannot have spaces. We do not
want you to change your column names within Airtable, but in "Gatsby-land" you
will need to always use the "cleaned" key. See https://github.com/jbolda/gatsby-source-airtable#column-names
for more information.`

exports.sourceNodes = async (
  { actions, createNodeId, store, cache },
  { apiKey, tables, concurrency, cleanKey }
) => {
  // tables contain baseId, tableName, tableView, queryName, mapping, tableLinks
  const { createNode, setPluginStatus } = actions;

  try {
    // hoist api so we can use in scope outside of this block
    if (!apiKey && process.env.GATSBY_AIRTABLE_API_KEY) {
      console.warn(
        "\nImplicit setting of GATSBY_AIRTABLE_API_KEY as apiKey will be deprecated in future release, apiKey should be set in gatsby-config.js, please see Readme!"
      );
    }
    var api = await new Airtable({
      apiKey: process.env.GATSBY_AIRTABLE_API_KEY || apiKey
    });
  } catch (e) {
    // airtable uses `assert` which doesn't exit the process,
    //  but rather just makes gatsby hang. Warn, don't create any
    //  nodes, but let gatsby continue working
    console.warn("\nAPI key is required to connect to Airtable");
    return;
  }

  // exit if tables is not defined
  if (tables === undefined || tables.length === 0) {
    console.warn(
      "\ntables is not defined for gatsby-source-airtable in gatsby-config.js"
    );
    return;
  }

  if (concurrency === undefined) {
    // Airtable hasn't documented what the rate limit against their attachment servers is.
    // They do document that API calls are limited to 5 requests/sec, so the default limit of 5 concurrent
    // requests for remote files has been selected in that spirit. A higher value can be set as a plugin
    // option in gatsby-config.js
    concurrency = 5;
  }

  // A user-defined function may be provided for `cleanKey`, otherwise a default
  // will be used when `undefined`
  if (cleanKey === undefined) {
    // Default cleanKey function replaces spaces with underscores
    cleanKey = key => key.replace(/ /g, "_");
  } else if (typeof cleanKey !== "function") {
    console.warn(
      "\ncleanKey must be a function or undefined to use the default value"
    );
    return;
  }

  console.time(`\nfetch all Airtable rows from ${tables.length} tables`);

  let queue = [];
  tables.forEach(tableOptions => {
    let base = api.base(tableOptions.baseId);

    let table = base(tableOptions.tableName);

    let view = tableOptions.tableView || "";

    let query = table.select({
      view: view
    });


    // Confirm that the user is using the clean keys. If they are not, warn them
    const isDirty = key => key.indexOf(' ') >= 0;
    const dirtyMappings = tableOptions.mapping
      ? Object.keys(tableOptions.mapping).filter(isDirty)
      : [];
    const dirtyLinks = tableOptions.tableLinks
      ? tableOptions.tableLinks.filter(isDirty)
      : [];

    if (dirtyMappings.length) console.warn(`
      ${DIRTY_WARNING}
      On the ${tableOptions.tableName} base ${tableOptions.baseId} 'mapping',
      you must use the cleaned name for ${dirtyMappings.join(', ')}
    `);

    if (dirtyLinks.length) console.warn(`
      ${DIRTY_WARNING}
      On the ${tableOptions.tableName} base ${tableOptions.baseId} 'tableLinks',
      you must use the cleaned name for ${dirtyLinks.join(', ')}
    `);

    // Short-circuit if any keys are dirty, after giving warnings for both
    // `mappings` and `tableLinks`
    if (dirtyMappings.length || dirtyLinks.length) return

    // query.all() returns a promise, pass an array for each table with
    // both our promise and the queryName and then map reduce at the
    // final promise resolution to get queryName onto each row
    queue.push(
      Promise.all([
        query.all(),
        tableOptions.queryName,
        tableOptions.defaultValues || {},
        typeof tableOptions.separateNodeType !== "undefined"
          ? tableOptions.separateNodeType
          : false,
        typeof tableOptions.separateMapType !== "undefined"
          ? tableOptions.separateMapType
          : false,
        cleanMapping,
        cleanLinks
      ])
    );
  });

  // queue has array of promises and when resolved becomes nested arrays
  // we flatten the array to return all rows from all tables after mapping
  // the queryName to each row
  const allRows = await Promise.all(queue)
    .then(all => {
      return all.reduce((accumulator, currentValue) => {
        return accumulator.concat(
          currentValue[0].map(row => {
            row.queryName = currentValue[1]; // queryName from tableOptions above
            row.defaultValues = currentValue[2]; // mapping from tableOptions above
            row.separateNodeType = currentValue[3]; // separateMapType from tableOptions above
            row.separateMapType = currentValue[4]; // create separate node type from tableOptions above
            row.mapping = currentValue[5]; // mapping from tableOptions above
            row.tableLinks = currentValue[6]; // tableLinks from tableOptions above
            return row;
          })
        );
      }, []);
    })
    .catch(e => {
      throw e;
      return;
    });

  console.timeEnd(`\nfetch all Airtable rows from ${tables.length} tables`);

  setPluginStatus({
    status: {
      lastFetched: new Date().toJSON()
    }
  });

  // Use the map function for arrays of promises imported from Bluebird.
  // Using the concurrency option protects against being blocked from Airtable's
  // file attachment servers for large numbers of requests.
  return map(
    allRows,
    async row => {
      // don't love mutating the row here, but
      // not ready to refactor yet to clean this up
      // (happy to take a PR!)
      row.fields = {
        ...row.defaultValues,
        ...row.fields
      };
      let processedData = await processData(row, {
        createNodeId,
        createNode,
        store,
        cache,
        cleanKey
      });

      if (row.separateNodeType && (!row.queryName || row.queryName === "")) {
        console.warn(
          `You have opted into separate node types, but not specified a queryName.
          We use the queryName to suffix to node type. Without a queryName, it will act like separateNodeType is false.`
        );
      }

      const node = {
        id: createNodeId(`Airtable_${row.id}`),
        parent: null,
        table: row._table.name,
        recordId: row.id,
        queryName: row.queryName,
        children: [],
        internal: {
          type: `Airtable${
            row.separateNodeType ? cleanType(row.queryName) : ""
          }`,
          contentDigest: crypto
            .createHash("md5")
            .update(JSON.stringify(row))
            .digest("hex")
        },
        data: processedData.data
      };

      createNode(node);

      await Promise.all(processedData.childNodes).then(nodes => {
        nodes.forEach(node => createNode(node));
      });
    },
    { concurrency: concurrency }
  );
};

const processData = async (row, { createNodeId, createNode, store, cache, cleanKey }) => {
  let data = row.fields;
  let tableLinks = row.tableLinks;
  let fieldKeys = Object.keys(data);
  let processedData = {};
  let childNodes = [];

  fieldKeys.forEach(key => {
    // once in "Gatsby-land" we want to use the cleanKey
    // consistently everywhere including in configs
    // this key that we clean comes from Airtable
    // at this point, all user option keys should be clean
    const cleanedKey = cleanKey(key);

    let useKey;
    // deals with airtable linked fields,
    // these will be airtable IDs
    if (tableLinks && tableLinks.includes(cleanedKey)) {
      useKey = `${cleanedKey}___NODE`;

      // `data` is direct from Airtable so we don't use
      // the cleanKey here
      processedData[useKey] = data[key].map(id =>
        createNodeId(`Airtable_${id}`)
      );
    } else if (row.mapping && row.mapping[cleanedKey]) {
      // A child node comes from the mapping, where we want to
      // define a separate node in gatsby that is available
      // for transforming. This will let other plugins pick
      // up on that node to add nodes.
      // row contains `data` which is direct from Airtable
      // so we pass down the raw instead of the cleanKey here
      let checkedChildNode = checkChildNode(key, row, processedData, {
        createNodeId,
        createNode,
        store,
        cache,
        cleanKey
      });
      childNodes.push(checkedChildNode);
    } else {
      // `data` is direct from Airtable so we don't use
      // the cleanKey here
      processedData[cleanedKey] = data[key];
    }
  });

  // wait for all of the children to finish
  await Promise.all(childNodes);
  // where childNodes returns an array of objects
  return { data: processedData, childNodes: childNodes };
};

const checkChildNode = async (
  key,
  row,
  processedData,
  { createNodeId, createNode, store, cache, cleanKey }
) => {
  let data = row.fields;
  let mapping = row.mapping;
  let cleanedKey = cleanKey(key);
  let localFiles = await localFileCheck(key, row, {
    createNodeId,
    createNode,
    store,
    cache,
    cleanKey
  });

  processedData[`${cleanedKey}___NODE`] = createNodeId(
    `AirtableField_${row.id}_${cleanedKey}`
  );

  return buildNode(
    localFiles,
    row,
    cleanedKey,
    data[key],
    mapping[key],
    createNodeId,
    cleanKey
  );
};

const localFileCheck = async (
  key,
  row,
  { createNodeId, createNode, store, cache, cleanKey }
) => {
  let data = row.fields;
  let mapping = row.mapping;
  let cleanedKey = cleanKey(key);
  if (mapping[cleanedKey] === `fileNode`) {
    try {
      let fileNodes = [];
      // where data[key] is the array of attachments
      // `data` is direct from Airtable so we don't use
      // the cleanKey here
      data[key].forEach(attachment => {
        let attachmentNode = createRemoteFileNode({
          url: attachment.url,
          store,
          cache,
          createNode,
          createNodeId
        });
        fileNodes.push(attachmentNode);
      });
      // Adds a field `localFile` to the node
      // ___NODE tells Gatsby that this field will link to another nodes
      const resolvedFileNodes = await Promise.all(fileNodes);
      const localFiles = resolvedFileNodes.map(
        attachmentNode => attachmentNode.id
      );
      return localFiles;
    } catch (e) {
      console.log(
        "You specified a fileNode, but we caught an error. First check that you have gatsby-source-filesystem installed?\n",
        e
      );
    }
  }
  return;
};

const buildNode = (localFiles, row, cleanedKey, raw, mapping, createNodeId, cleanKey) => {
  const nodeType = row.separateNodeType
    ? `Airtable${cleanKey(row.queryName ? row.queryName : row._table.name)}`
    : `Airtable`;
  if (localFiles) {
    return {
      id: createNodeId(`AirtableField_${row.id}_${cleanedKey}`),
      parent: createNodeId(`Airtable_${row.id}`),
      children: [],
      raw: raw,
      localFiles___NODE: localFiles,
      internal: {
        type: `AirtableField${row.separateMapType ? cleanType(mapping) : ""}`,
        mediaType: mapping,
        content: typeof raw === "string" ? raw : JSON.stringify(raw),
        contentDigest: crypto
          .createHash("md5")
          .update(JSON.stringify(row))
          .digest("hex")
      }
    };
  } else {
    return {
      id: createNodeId(`AirtableField_${row.id}_${cleanedKey}`),
      parent: createNodeId(`Airtable_${row.id}`),
      children: [],
      raw: raw,
      internal: {
        type: `AirtableField${row.separateMapType ? cleanType(mapping) : ""}`,
        mediaType: mapping,
        content: typeof raw === "string" ? raw : JSON.stringify(raw),
        contentDigest: crypto
          .createHash("md5")
          .update(JSON.stringify(row))
          .digest("hex")
      }
    };
  }
};

const cleanType = key => {
  return !key ? "" : key.replace(/[ /+]/g, "");
};
