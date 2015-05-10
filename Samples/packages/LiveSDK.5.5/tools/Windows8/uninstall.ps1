param($installPath, $toolsPath, $package, $project)

switch ($project.Type) {
    "C#" {
        $project.Object.References | Where-Object { $_.Name -eq 'Microsoft.Live' } | ForEach-Object { $_.Remove() }
    }
    "JavaScript" {
        try {
            $jsFolderProjectItem = $project.ProjectItems.Item("js")
        } catch {
            write-host "Unable to find js directory."
            return
        }

        try {
            $jsFileProjectItem = $jsFolderProjectItem.ProjectItems.Item("wl.js")
        } catch {
            write-host "Unable to find wl.js."
            return
        }

        $jsFileProjectItem.Remove()
    }
}
