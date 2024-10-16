use async_trait::async_trait;
use clap::{Arg, Command};
use std::path::Path;

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;

use crate::cmd::RunCmd;
pub const NAME: &str = "add";

pub struct SchemasAddCmd;

#[async_trait]
impl RunCmd for SchemasAddCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Stage a schema to a data frame to be committed.")
            .arg(Arg::new("PATH").help("The path of the data frame file."))
            .arg(
                Arg::new("column")
                    .long("column")
                    .short('c')
                    .help("The column that you want to override the data type or metadata for."),
            )
            .arg(
                Arg::new("metadata")
                    .long("metadata")
                    .short('m')
                    .help("Set the metadata for a specific column. Must pass in the -c flag."),
            )
            .arg(
                Arg::new("render")
                    .long("render")
                    .help("Set the render type for a specific column. Supported render types: (image, link). Must pass in the -c flag."),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        // Path
        let path = args.get_one::<String>("PATH").map(Path::new);

        // Flags
        let column = args.get_one::<String>("column");
        let metadata = args.get_one::<String>("metadata");
        let render = args.get_one::<String>("render");

        let err_msg = "Must supply a file path, column name and either -m for metadata or -t for data type\n\n  oxen schemas add file.csv -c 'col1' -t 'str'\n";

        let Some(path) = path else {
            return Err(OxenError::basic_str(err_msg));
        };

        // If there is a render flag without a column, return an error
        if render.is_some() && column.is_none() {
            return Err(OxenError::basic_str(
                "Must supply a column name with the -c flag when using --render.",
            ));
        }

        // Find the repo
        let repository = LocalRepository::from_current_dir()?;

        // If a column is supplied, then we need to supply a data type or metadata for that column
        if let Some(column) = column {
            if let Some(render) = render {
                let render_json = self.generate_render_json(render)?;
                match self.schema_add_column_metadata(&repository, path, column, render_json) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }

            if let Some(metadata) = metadata {
                match self.schema_add_column_metadata(&repository, path, column, metadata) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
        } else {
            // No column, check if we are just adding metadata to the schema
            if let Some(metadata) = metadata {
                match self.schema_add_metadata(&repository, path, metadata) {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}")
                    }
                }
            }
        }

        Ok(())
    }
}

impl SchemasAddCmd {
    fn schema_add_column_metadata(
        &self,
        repository: &LocalRepository,
        path: impl AsRef<Path>,
        column: impl AsRef<str>,
        metadata: impl AsRef<str>,
    ) -> Result<(), OxenError> {
        // make sure metadata is valid json, return oxen error if not
        let metadata: serde_json::Value = serde_json::from_str(metadata.as_ref()).map_err(|e| {
            OxenError::basic_str(format!(
                "Metadata must be valid JSON: '{}'\n{}",
                metadata.as_ref(),
                e
            ))
        })?;

        for (path, schema) in repositories::data_frames::schemas::add_column_metadata(
            repository, path, column, &metadata,
        )? {
            println!("{:?}\n{}", path, schema.verbose_str());
        }

        Ok(())
    }

    fn schema_add_metadata(
        &self,
        repository: &LocalRepository,
        path: impl AsRef<Path>,
        metadata: impl AsRef<str>,
    ) -> Result<(), OxenError> {
        let metadata: serde_json::Value = serde_json::from_str(metadata.as_ref()).map_err(|e| {
            OxenError::basic_str(format!(
                "Metadata must be valid JSON: '{}'\n{}",
                metadata.as_ref(),
                e
            ))
        })?;

        for (path, schema) in
            repositories::data_frames::schemas::add_schema_metadata(repository, path, &metadata)?
        {
            println!("{:?}\n{}", path, schema.verbose_str());
        }

        Ok(())
    }

    fn generate_render_json(&self, render_type: impl AsRef<str>) -> Result<String, OxenError> {
        let render_type = render_type.as_ref();
        if render_type == "image" || render_type == "link" {
            let json = serde_json::json!({
                "_oxen": {
                  "render": {
                      "func": render_type
                  }
                }
            });

            Ok(serde_json::to_string(&json)?)
        } else {
            Err(OxenError::basic_str(format!(
                "Invalid render type: {}",
                render_type
            )))
        }
    }
}
