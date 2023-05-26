version 1.0

workflow tnu_head_n_by_m_scatter {
    input {
        Array[String] input_files
        Int scatter_input_size
        String workspace_namespace
        String workspace_name
        String workspace_google_project
        String workspace_bucket
    }

    call chunk_array {
         input: input_array=input_files, chunk_size=scatter_input_size
    }

    scatter (input_file_chunk in chunk_array.output_array_array) {
       call tnu_heads { input: input_files=input_file_chunk, 
       workspace_namespace=workspace_namespace, 
       workspace_name=workspace_name, 
       workspace_google_project=workspace_google_project, 
       workspace_bucket=workspace_bucket}
   }

   output {
       Array[String] tnu_heads_output_strings = flatten(select_all(tnu_heads.std_output))
       # File tnu_heads_output_file = write_lines(tnu_heads_output_strings)
   }
}

task chunk_array {
    input {
        Array[String] input_array
        Int chunk_size
    }

    String tsv_filename = "./chunked_array.tsv"
    command <<<
        set -eux -o pipefail

        # Chunk the array into a TSV file using Python
        python3 <<CODE
        count:int = 0
        content:str = ""
        for value in ["~{sep='", "' input_array}"]:
          count += 1
          if count > 1:
              content += "\t"
          content += value
          if count >= ~{chunk_size}:
            content += "\n"
            count=0
        # Ensure the content ends with a newline
        if content[-1] != "\n":
            content += "\n"

        with open("~{tsv_filename}", "w") as fh:
            fh.write(content)
        CODE

        # Debug
        echo ~{tsv_filename}:
        cat ~{tsv_filename}
    >>>

    output {
       File chunked_array_tsv = "./chunked_array.tsv"
       Array[Array[String]] output_array_array = read_tsv(chunked_array_tsv)
    }

    runtime {
      docker: "python:3.9-bullseye"
      cpu: 2
      memory: "8 GB"
      disks: "local-disk 30 HDD"
    }
}

task tnu_heads {
    input {
        Array[String] input_files
        String workspace_namespace
        String workspace_name
        String workspace_google_project
        String workspace_bucket
       
    }

    command <<<
        set -eux -o pipefail
        
        env
        
        # Define env vars required/used by terra-notebook-utils
        export WORKSPACE_NAMESPACE=~{workspace_namespace}
        export WORKSPACE_NAME=~{workspace_name}
        export GOOGLE_PROJECT=~{workspace_google_project}
        export WORKSPACE_BUCKET=~{workspace_bucket}
        
        env
        
        pip install --upgrade --no-cache-dir terra-notebook-utils

        python3 <<CODE

        from terra_notebook_utils import drs

        for drs_uri in ["~{sep='", "' input_files}"]:
            try:
                drs_info = drs.info(drs_uri)
                drs.head(drs_uri, 10)
                output = f"{drs_uri}: Successful {drs_info}"
            except Exception as ex:
                output = f"{drs_uri}: Failure: {ex}"

            print(output)
        CODE
    >>>

     output {
        Array[String] std_output = read_lines(stdout())
     }

     runtime {
       docker: "python:3.9-bullseye"
       cpu: 2
       memory: "8 GB"
       disks: "local-disk 30 HDD"
     }
}
