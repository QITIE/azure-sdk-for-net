// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for
// license information.
// 
// Code generated by Microsoft (R) AutoRest Code Generator 0.14.0.0
// Changes may cause incorrect behavior and will be lost if the code is
// regenerated.

namespace Microsoft.Azure.Management.ResourceManager.Models
{
    using System;
    using System.Linq;
    using System.Collections.Generic;
    using Newtonsoft.Json;
    using Microsoft.Rest;
    using Microsoft.Rest.Serialization;
    using Microsoft.Rest.Azure;

    /// <summary>
    /// Resource filter.
    /// </summary>
    public partial class GenericResourceFilter
    {
        /// <summary>
        /// Initializes a new instance of the GenericResourceFilter class.
        /// </summary>
        public GenericResourceFilter() { }

        /// <summary>
        /// Initializes a new instance of the GenericResourceFilter class.
        /// </summary>
        public GenericResourceFilter(string resourceType = default(string), string tagname = default(string), string tagvalue = default(string))
        {
            ResourceType = resourceType;
            Tagname = tagname;
            Tagvalue = tagvalue;
        }

        /// <summary>
        /// Gets or sets the resource type.
        /// </summary>
        [JsonProperty(PropertyName = "resourceType")]
        public string ResourceType { get; set; }

        /// <summary>
        /// Gets or sets the tag name.
        /// </summary>
        [JsonProperty(PropertyName = "tagname")]
        public string Tagname { get; set; }

        /// <summary>
        /// Gets or sets the tag value.
        /// </summary>
        [JsonProperty(PropertyName = "tagvalue")]
        public string Tagvalue { get; set; }

    }
}
